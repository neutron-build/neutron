//! Type-kernel rules suite (card C-Q3, work item 3): hand-written vectors
//! pinned to C-Q3s §4-§7, run through the kernels (`compare`, `eq`, `hash`,
//! `in_bounds`, `compare_key`) and the registry rows.
//!
//! Vectors are written as chains of *rank groups* in SQL literal syntax (the
//! same literal parsers the golden corpus uses): every value in a group is
//! equal to its group mates, and every group is strictly below the next.
//! The chains are written from the PostgreSQL rules the spec cites, not read
//! from the encoder.

mod common;

use std::cmp::Ordering;
use std::ops::Bound;

use common::{parse_row, type_for};
use nucleus_codec::registry::{self, kind_of, row_of, TypeKind, TYPE_OPS};
use nucleus_codec::{
    compare, compare_key, eq, hash, in_bounds, ops, try_compare, try_eq, try_hash, try_in_bounds,
    Array, ArrayDim, CodecError, Collation, Decimal, Direction, Interval, Jsonb, KeyColumn,
    KeyType, Nulls, Numeric, Value, OP_BTREE_CMP, OP_EQ, OP_HASH,
};

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// One non-NULL value from a golden-style SQL literal.
fn lit(stem: &str, s: &str) -> Value {
    parse_row(stem, s)
        .into_iter()
        .next()
        .flatten()
        .unwrap_or_else(|| panic!("{stem}: {s} parsed to NULL"))
}

/// `a = b` for every kernel, in both argument orders, through the registry
/// row as well, and equal hashes (P-EQ, P-HASH).
fn assert_same(ty: &KeyType, a: &Value, b: &Value) {
    let row = ops(ty);
    for (x, y) in [(a, b), (b, a)] {
        assert_eq!(compare(ty, x, y), Ordering::Equal, "{ty:?}: {x:?} vs {y:?}");
        assert_eq!(try_compare(ty, x, y), Ok(Ordering::Equal), "{x:?} vs {y:?}");
        assert!(eq(ty, x, y), "{ty:?}: {x:?} should equal {y:?}");
        assert_eq!(try_eq(ty, x, y), Ok(true));
        assert_eq!((row.compare)(ty, x, y), Ok(Ordering::Equal));
        assert_eq!((row.eq)(ty, x, y), Ok(true));
        assert_eq!(hash(ty, x), hash(ty, y), "{ty:?}: hash {x:?} vs {y:?}");
        assert_eq!((row.hash)(ty, x), (row.hash)(ty, y));
        assert!(in_bounds(ty, x, Bound::Included(y), Bound::Included(y)));
        assert!(!in_bounds(ty, x, Bound::Excluded(y), Bound::Unbounded));
        assert!(!in_bounds(ty, x, Bound::Unbounded, Bound::Excluded(y)));
    }
}

/// `a < b` for every kernel, both argument orders (antisymmetry).
fn assert_less(ty: &KeyType, a: &Value, b: &Value) {
    let row = ops(ty);
    assert_eq!(compare(ty, a, b), Ordering::Less, "{ty:?}: {a:?} < {b:?}");
    assert_eq!(
        compare(ty, b, a),
        Ordering::Greater,
        "{ty:?}: {b:?} > {a:?}"
    );
    assert_eq!((row.compare)(ty, a, b), Ok(Ordering::Less));
    assert_eq!((row.compare)(ty, b, a), Ok(Ordering::Greater));
    assert!(!eq(ty, a, b), "{ty:?}: {a:?} must not equal {b:?}");
    assert!(!eq(ty, b, a));
    assert_eq!((row.eq)(ty, a, b), Ok(false));
    // Index-scan bounds follow the same order.
    assert!(in_bounds(ty, a, Bound::Unbounded, Bound::Excluded(b)));
    assert!(!in_bounds(ty, b, Bound::Unbounded, Bound::Excluded(b)));
    assert!(in_bounds(ty, b, Bound::Excluded(a), Bound::Unbounded));
    assert!(!in_bounds(ty, a, Bound::Excluded(a), Bound::Unbounded));
    assert!(!in_bounds(ty, b, Bound::Unbounded, Bound::Included(a)));
}

/// `groups[i]` all equal, `groups[i] < groups[j]` for `i < j`.
fn assert_chain(ty: &KeyType, groups: &[Vec<Value>]) {
    for (i, gi) in groups.iter().enumerate() {
        assert!(!gi.is_empty());
        for a in gi {
            for b in gi {
                assert_same(ty, a, b);
            }
        }
        for gj in &groups[i + 1..] {
            for a in gi {
                for b in gj {
                    assert_less(ty, a, b);
                }
            }
        }
    }
}

/// Chain from literals of one golden stem.
fn chain(stem: &str, groups: &[&[&str]]) {
    let ty = type_for(stem);
    let vals: Vec<Vec<Value>> = groups
        .iter()
        .map(|g| g.iter().map(|s| lit(stem, s)).collect())
        .collect();
    assert_chain(&ty, &vals);
}

fn dec(negative: bool, digits: &[u8], scale: i32) -> Value {
    Value::Numeric(Numeric::Finite(Decimal {
        negative,
        digits: digits.to_vec(),
        scale,
    }))
}

fn int4_array(dims: &[(i32, i32)], elems: Vec<Option<i32>>) -> Value {
    Value::Array(Array {
        dims: dims
            .iter()
            .map(|&(len, lower)| ArrayDim { len, lower })
            .collect(),
        elems: elems.into_iter().map(|e| e.map(Value::Int4)).collect(),
    })
}

fn text_array(elems: &[Option<&str>]) -> Value {
    Value::Array(Array::from_elems(
        elems
            .iter()
            .map(|e| e.map(|s| Value::Text(s.to_string())))
            .collect(),
    ))
}

// ---------------------------------------------------------------------------
// §4 scalars
// ---------------------------------------------------------------------------

#[test]
fn bool_false_below_true() {
    chain("bool", &[&["false", "f", "FALSE"], &["true", "t"]]);
}

#[test]
fn integers_follow_signed_order_at_the_extremes() {
    let cases: [(KeyType, [Value; 5]); 3] = [
        (
            KeyType::Int2,
            [i16::MIN, -1, 0, 1, i16::MAX].map(Value::Int2),
        ),
        (
            KeyType::Int4,
            [i32::MIN, -1, 0, 1, i32::MAX].map(Value::Int4),
        ),
        (
            KeyType::Int8,
            [i64::MIN, -1, 0, 1, i64::MAX].map(Value::Int8),
        ),
    ];
    for (ty, vals) in cases {
        let groups: Vec<Vec<Value>> = vals.iter().map(|v| vec![v.clone()]).collect();
        assert_chain(&ty, &groups);
    }
}

#[test]
fn float8_order_zero_and_nan() {
    // -Inf < negatives < -0 = 0 < positives < +Inf < NaN, NaN = NaN (§4.1).
    chain(
        "float8",
        &[
            &["-Infinity"],
            &["-1.7976931348623157e308"],
            &["-2.5"],
            &["-1"],
            &["-5e-324"],
            &["-0", "0", "0.0"],
            &["5e-324"],
            &["1", "1.0"],
            &["1.7976931348623157e308"],
            &["Infinity"],
            &["NaN"],
        ],
    );
    let ty = KeyType::Float8;
    // Negative zero is a value of its own bit pattern but equal to +0.
    assert_same(&ty, &Value::Float8(-0.0), &Value::Float8(0.0));
    // Every NaN (sign, payload, signalling) is the one NaN, above +Inf.
    let nans = [
        f64::NAN,
        -f64::NAN,
        f64::from_bits(0x7FF0_0000_0000_0001),
        f64::from_bits(0xFFF8_0000_0000_0000),
        f64::from_bits(0xFFFF_FFFF_FFFF_FFFF),
    ];
    for x in nans {
        assert!(x.is_nan());
        for y in nans {
            assert_same(&ty, &Value::Float8(x), &Value::Float8(y));
        }
        assert_less(&ty, &Value::Float8(f64::INFINITY), &Value::Float8(x));
        assert_less(&ty, &Value::Float8(f64::MAX), &Value::Float8(x));
    }
}

#[test]
fn float4_order_zero_and_nan() {
    chain(
        "float4",
        &[
            &["-Infinity"],
            &["-3.4028235e38"],
            &["-1.5"],
            &["-1e-45"],
            &["-0", "0"],
            &["1e-45"],
            &["1.5"],
            &["3.4028235e38"],
            &["Infinity"],
            &["NaN"],
        ],
    );
    let ty = KeyType::Float4;
    assert_same(&ty, &Value::Float4(-0.0), &Value::Float4(0.0));
    let nans = [
        f32::NAN,
        -f32::NAN,
        f32::from_bits(0x7F80_0001),
        f32::from_bits(0xFFC0_0000),
    ];
    for x in nans {
        assert!(x.is_nan());
        for y in nans {
            assert_same(&ty, &Value::Float4(x), &Value::Float4(y));
        }
        assert_less(&ty, &Value::Float4(f32::INFINITY), &Value::Float4(x));
    }
}

#[test]
fn numeric_scale_is_not_part_of_equality() {
    // 1.0 = 1.00 = 1 = 01; 0 = -0 = 0.000; order across the rest (§4.2).
    chain(
        "numeric",
        &[
            &["-Infinity"],
            &["-1000000000000000000000000000000000000000"],
            &["-123.456"],
            &["-1.5", "-1.50"],
            &["-1.0001"],
            &["-1", "-1.0", "-01"],
            &["-0.5", "-0.50"],
            &["-0.0000001"],
            &["0", "-0", "0.0", "0.000", "000"],
            &["0.0000001"],
            &["0.5", "0.50"],
            &["1", "1.0", "1.00", "01"],
            &["1.0001"],
            &["10", "10.0", "10.00"],
            &["100", "100.0"],
            &["1000", "1000.000"],
            &["123456789012345678901234567890"],
            &["123456789012345678901234567891"],
            &["Infinity"],
            &["NaN"],
        ],
    );
    let ty = KeyType::Numeric;
    // Same value through different (digits, scale) shapes, including a
    // negative scale (1e3 = 1000 = 1000.00) and padded zeros.
    let thousand = [
        dec(false, &[1], -3),
        dec(false, &[1, 0, 0, 0], 0),
        dec(false, &[1, 0, 0, 0, 0, 0], 2),
        dec(false, &[0, 0, 1, 0, 0, 0], 0),
    ];
    for a in &thousand {
        for b in &thousand {
            assert_same(&ty, a, b);
        }
    }
    // A zero with any digit string or scale, negative flag or not.
    let zeros = [
        dec(false, &[], 0),
        dec(true, &[], 5),
        dec(true, &[0, 0, 0], -3),
        dec(false, &[0], 17),
    ];
    for a in &zeros {
        for b in &zeros {
            assert_same(&ty, a, b);
        }
    }
    assert_less(&ty, &zeros[0], &dec(false, &[1], 6));
    assert_less(&ty, &dec(true, &[1], 6), &zeros[0]);
    // NaN = NaN above +Infinity; -Infinity below the most negative finite.
    assert_same(
        &ty,
        &Value::Numeric(Numeric::NaN),
        &Value::Numeric(Numeric::NaN),
    );
    assert_less(
        &ty,
        &Value::Numeric(Numeric::PosInf),
        &Value::Numeric(Numeric::NaN),
    );
    assert_less(
        &ty,
        &Value::Numeric(Numeric::NegInf),
        &dec(true, &[9, 9, 9], -100_000),
    );
    assert_less(
        &ty,
        &dec(false, &[9, 9, 9], -100_000),
        &Value::Numeric(Numeric::PosInf),
    );
}

#[test]
fn numeric_orders_by_magnitude_not_by_digit_string() {
    let ty = KeyType::Numeric;
    // 9 < 10 although "9" > "10" as strings; 0.9 < 0.95 < 1.
    assert_less(&ty, &lit("numeric", "9"), &lit("numeric", "10"));
    assert_less(&ty, &lit("numeric", "0.9"), &lit("numeric", "0.95"));
    assert_less(&ty, &lit("numeric", "0.99"), &lit("numeric", "1"));
    // Negatives mirror: -10 < -9, and a longer mantissa is smaller for them.
    assert_less(&ty, &lit("numeric", "-10"), &lit("numeric", "-9"));
    assert_less(&ty, &lit("numeric", "-0.95"), &lit("numeric", "-0.9"));
    // Huge exponents on both sides of the decimal point.
    assert_less(&ty, &dec(false, &[1], 100_000), &dec(false, &[1], 99_999));
    assert_less(&ty, &dec(false, &[1], -99_999), &dec(false, &[1], -100_000));
}

#[test]
fn interval_orders_by_total_microseconds() {
    let ty = KeyType::Interval;
    chain(
        "interval",
        &[
            &["-infinity"],
            &["-100 years"],
            &["-1 mon", "-30 days", "-720 hours"],
            &["-1 day -1 microsecond"],
            &["-1 day", "-24 hours", "-1440 minutes"],
            &["-1 hour", "-60 minutes"],
            &["0", "0 seconds", "1 mon -30 days", "-0 seconds"],
            &["1 microsecond"],
            &["1 second", "1000000 microseconds"],
            &["1 hour", "60 minutes"],
            &["1 day", "24 hours"],
            &["1 day 1 microsecond"],
            &["29 days 23 hours"],
            &["1 mon", "30 days", "720 hours"],
            &["1 year", "360 days", "12 mons"],
            &["infinity"],
        ],
    );
    // The infinities are the unique extremes, whatever the finite fields.
    let near_max = Value::Interval(Interval {
        months: i32::MAX,
        days: i32::MAX,
        micros: i64::MAX - 1,
    });
    let near_min = Value::Interval(Interval {
        months: i32::MIN,
        days: i32::MIN,
        micros: i64::MIN + 1,
    });
    assert_less(&ty, &near_max, &Value::Interval(Interval::INFINITY));
    assert_less(&ty, &Value::Interval(Interval::NEG_INFINITY), &near_min);
    assert_same(
        &ty,
        &Value::Interval(Interval::INFINITY),
        &Value::Interval(Interval::INFINITY),
    );
    // Different field splits of one span are equal.
    let day = Interval::USECS_PER_DAY;
    let spans = [
        Interval {
            months: 1,
            days: 0,
            micros: 0,
        },
        Interval {
            months: 0,
            days: 30,
            micros: 0,
        },
        Interval {
            months: 0,
            days: 0,
            micros: 30 * day,
        },
        Interval {
            months: 2,
            days: -30,
            micros: 0,
        },
        Interval {
            months: 0,
            days: 31,
            micros: -day,
        },
    ];
    for a in spans {
        for b in spans {
            assert_same(&ty, &Value::Interval(a), &Value::Interval(b));
        }
    }
}

// ---------------------------------------------------------------------------
// §5 text, varchar, bytea
// ---------------------------------------------------------------------------

#[test]
fn text_is_bytewise_with_prefix_first() {
    chain(
        "text",
        &[
            &["''"],
            &["' '"],
            &["'0'"],
            &["'A'"],
            &["'Ab'"],
            &["'B'"],
            &["'a'"],
            &["'a b'"],
            &["'ab'"],
            &["'ab '"],
            &["'abc'"],
            &["'b'"],
            &["'z'"],
            &["'zz'"],
            &["'~'"],
            &["'é'"],
            &["'ÿ'"],
            &["'€'"],
            &["'😀'"],
        ],
    );
    // varchar(n) is text: same type, same order. Length-first would put "b"
    // below "aa"; bytewise must not.
    let ty = KeyType::Text(Collation::C);
    let t = |s: &str| Value::Text(s.to_string());
    assert_less(&ty, &t("aa"), &t("b"));
    assert_less(&ty, &t("a"), &t("a\u{1}"));
    // UTF-8 byte order is code point order across encoded lengths.
    assert_less(&ty, &t("\u{7f}"), &t("\u{80}"));
    assert_less(&ty, &t("\u{7ff}"), &t("\u{800}"));
    assert_less(&ty, &t("\u{ffff}"), &t("\u{10000}"));
    assert_less(&ty, &t("\u{10ffff}"), &t("\u{10ffff}\u{0}"));
    assert_same(&ty, &t("same"), &t("same"));
}

#[test]
fn bytea_nul_escapes_keep_prefix_order() {
    // \x < \x00 < \x0000 < \x0000ff < \x0001 < \x00ff < \x00ff00 < \x01 ...
    chain(
        "bytea",
        &[
            &["\\x"],
            &["\\x00"],
            &["\\x0000"],
            &["\\x0000ff"],
            &["\\x0001"],
            &["\\x00ff"],
            &["\\x00ff00"],
            &["\\x00ff01"],
            &["\\x01"],
            &["\\x0100"],
            &["\\x01ff"],
            &["\\x41"],
            &["\\x4142"],
            &["\\x42"],
            &["\\x7f"],
            &["\\x80"],
            &["\\xff"],
            &["\\xff00"],
            &["\\xffff"],
        ],
    );
    // The escape of a NUL is 00 ff and the terminator 00 01: a string that
    // ends right before a NUL must still sort below the one that continues.
    let ty = KeyType::Bytea;
    let b = |v: &[u8]| Value::Bytea(v.to_vec());
    assert_less(&ty, &b(&[]), &b(&[0]));
    assert_less(&ty, &b(&[7]), &b(&[7, 0]));
    assert_less(&ty, &b(&[7, 0]), &b(&[7, 0, 0]));
    assert_less(&ty, &b(&[7, 0, 0]), &b(&[7, 0, 1]));
    // Not length-first: two bytes of 0x01 are below one byte of 0x02.
    assert_less(&ty, &b(&[1, 1]), &b(&[2]));
    // Raw 0xff after 0x00 is data, not the escape continuation.
    assert_less(&ty, &b(&[0, 0xFF]), &b(&[0, 0xFF, 0]));
    assert_less(&ty, &b(&[0, 0xFF, 0]), &b(&[1]));
}

// ---------------------------------------------------------------------------
// date / time / timestamp / uuid
// ---------------------------------------------------------------------------

#[test]
fn date_extremities() {
    chain(
        "date",
        &[
            &["-infinity"],
            &["0001-01-01"],
            &["1969-07-20"],
            &["1999-12-31"],
            &["2000-01-01"],
            &["2000-01-02"],
            &["2000-02-29"],
            &["9999-12-31"],
            &["5874896-01-01"],
            &["infinity"],
        ],
    );
    let ty = KeyType::Date;
    assert_less(&ty, &Value::Date(i32::MIN), &Value::Date(i32::MIN + 1));
    assert_less(&ty, &Value::Date(i32::MAX - 1), &Value::Date(i32::MAX));
    assert_less(&ty, &Value::Date(-1), &Value::Date(0));
}

#[test]
fn time_domain_includes_24_hours() {
    chain(
        "time",
        &[
            &["00:00:00", "00:00:00.000000"],
            &["00:00:00.000001"],
            &["00:00:00.1", "00:00:00.100000"],
            &["00:00:01"],
            &["12:00:00"],
            &["12:00:00.000001"],
            &["23:59:59"],
            &["23:59:59.999999"],
            &["24:00:00"],
        ],
    );
    let ty = KeyType::Time;
    assert_less(
        &ty,
        &Value::Time(Interval::USECS_PER_DAY - 1),
        &Value::Time(Interval::USECS_PER_DAY),
    );
}

#[test]
fn timestamp_extremities() {
    chain(
        "timestamp",
        &[
            &["-infinity"],
            &["0001-01-01 00:00:00"],
            &["1899-12-31 23:59:59.999999"],
            &["1999-12-31 23:59:59.999999"],
            &["2000-01-01", "2000-01-01 00:00:00"],
            &["2000-01-01 00:00:00.000001"],
            &["2000-01-01 00:00:01"],
            &["9999-12-31 23:59:59.999999"],
            &["100000-01-01 00:00:00"],
            &["infinity"],
        ],
    );
    let ty = KeyType::Timestamp;
    assert_less(
        &ty,
        &Value::Timestamp(i64::MIN),
        &Value::Timestamp(i64::MIN + 1),
    );
    assert_less(
        &ty,
        &Value::Timestamp(i64::MAX - 1),
        &Value::Timestamp(i64::MAX),
    );
}

#[test]
fn timestamptz_compares_the_utc_instant() {
    // The same instant written in different zones is one value (§4).
    chain(
        "timestamptz",
        &[
            &["-infinity"],
            &[
                "1999-12-31 23:00:00+00",
                "2000-01-01 00:00:00+01",
                "1999-12-31 15:00:00-08",
            ],
            &[
                "2000-01-01 00:00:00+00",
                "2000-01-01 01:00:00+01",
                "1999-12-31 16:00:00-08",
            ],
            &["2000-01-01 00:00:00.000001+00"],
            &["2000-01-01 00:00:01+00", "2000-01-01 05:30:01+05:30"],
            &["infinity"],
        ],
    );
    let ty = KeyType::TimestampTz;
    assert_less(&ty, &Value::TimestampTz(i64::MIN), &Value::TimestampTz(0));
    assert_less(&ty, &Value::TimestampTz(0), &Value::TimestampTz(i64::MAX));
}

#[test]
fn uuid_is_memcmp_of_the_sixteen_bytes() {
    chain(
        "uuid",
        &[
            &["00000000-0000-0000-0000-000000000000"],
            &["00000000-0000-0000-0000-000000000001"],
            &[
                "00000000-0000-0000-0000-00000000000a",
                "00000000-0000-0000-0000-00000000000A",
            ],
            &["00000000-0000-0000-0000-000000000100"],
            &["00000000-0000-0000-0001-000000000000"],
            &["12345678-1234-5678-9abc-def012345678"],
            &["12345678-1234-5678-9abc-def012345679"],
            &["7fffffff-0000-0000-0000-000000000000"],
            &["80000000-0000-0000-0000-000000000000"],
            &["ffffffff-ffff-ffff-ffff-fffffffffff0"],
            &["ffffffff-ffff-ffff-ffff-ffffffffffff"],
        ],
    );
}

// ---------------------------------------------------------------------------
// §6 jsonb
// ---------------------------------------------------------------------------

#[test]
fn jsonb_type_order_counts_and_rule_6() {
    // Rule 6: top-level [] below every scalar, null included. Rule 1: string
    // < number < bool < array < object. Rule 2: counts before content, so
    // every 1-element array is below every 2-element array and every
    // 1-pair object below every 2-pair object.
    chain(
        "jsonb",
        &[
            &["[]"],
            &["null"],
            &["\"\""],
            &["\"a\""],
            &["\"ab\""],
            &["\"b\""],
            &["-1000"],
            &["-1", "-1.0"],
            &["0", "0.0", "-0"],
            &["0.5"],
            &["1", "1.0", "1.00"],
            &["100"],
            &["false"],
            &["true"],
            &["[null]"],
            &["[\"\"]"],
            &["[0]"],
            &["[1]", "[1.0]"],
            &["[2]"],
            &["[[]]"],
            &["[[1]]"],
            &["[[[[[1]]]]]"],
            &["[\"a\",\"\"]"],
            &["[1,0]"],
            &["[1,1]"],
            &["[1,2]"],
            &["{}"],
            &["{\"a\":0}"],
            &["{\"a\":1}", "{\"a\":1.0}"],
            &["{\"a\":2}", "{\"a\":1,\"a\":2}"],
            &["{\"ab\":1}"],
            &["{\"abc\":1}"],
            &["{\"a\":1,\"b\":2}", "{\"b\":2,\"a\":1}"],
            &["{\"b\":1,\"d\":1}"],
            &["{\"aa\":1,\"c\":1}"],
        ],
    );
}

#[test]
fn jsonb_rule_6_is_top_level_only() {
    let ty = KeyType::Jsonb;
    let empty = Value::Jsonb(Jsonb::Array(vec![]));
    let null = Value::Jsonb(Jsonb::Null);
    let one = Value::Jsonb(Jsonb::Array(vec![Jsonb::Number(Numeric::Finite(
        Decimal {
            negative: false,
            digits: vec![1],
            scale: 0,
        },
    ))]));
    let scalar_one = lit("jsonb", "1");
    assert_less(&ty, &empty, &null);
    assert_less(&ty, &scalar_one, &one);
    // Nested, [] follows rule 1 (array above every scalar), so [[]] > [null].
    let nested_empty = Value::Jsonb(Jsonb::Array(vec![Jsonb::Array(vec![])]));
    let nested_null = Value::Jsonb(Jsonb::Array(vec![Jsonb::Null]));
    let nested_true = Value::Jsonb(Jsonb::Array(vec![Jsonb::Bool(true)]));
    assert_less(&ty, &nested_null, &nested_empty);
    assert_less(&ty, &nested_true, &nested_empty);
}

#[test]
fn jsonb_counts_come_before_content() {
    let ty = KeyType::Jsonb;
    // ["a",""] (2 elements) > [[[[[1]]]]] (1 element) although "a" < [..].
    assert_less(
        &ty,
        &lit("jsonb", "[[[[[1]]]]]"),
        &lit("jsonb", "[\"a\",\"\"]"),
    );
    // [9] < [0,0]: one element against two, whatever the content.
    assert_less(&ty, &lit("jsonb", "[9]"), &lit("jsonb", "[0,0]"));
    assert_less(
        &ty,
        &lit("jsonb", "{\"z\":9}"),
        &lit("jsonb", "{\"a\":0,\"b\":0}"),
    );
    // Equal counts then compare element by element.
    assert_less(&ty, &lit("jsonb", "[1,9]"), &lit("jsonb", "[2,0]"));
}

#[test]
fn jsonb_object_keys_compare_in_storage_order() {
    let ty = KeyType::Jsonb;
    // Storage order is (length, bytes): the first stored keys are "c" vs "b".
    assert_less(
        &ty,
        &lit("jsonb", "{\"b\":1,\"d\":1}"),
        &lit("jsonb", "{\"aa\":1,\"c\":1}"),
    );
    // Input order and duplicate keys do not matter (last duplicate wins).
    assert_same(
        &ty,
        &lit("jsonb", "{\"a\":1,\"b\":2}"),
        &lit("jsonb", "{\"b\":2,\"a\":1}"),
    );
    assert_same(
        &ty,
        &lit("jsonb", "{\"a\":1,\"a\":2}"),
        &lit("jsonb", "{\"a\":2}"),
    );
    // Storage order only decides which pair is compared first; two keys
    // compare as strings (bytewise), so "aa" < "z" although "z" is shorter.
    assert_less(&ty, &lit("jsonb", "{\"aa\":1}"), &lit("jsonb", "{\"z\":1}"));
    // Storage order puts "b" before "zz" on the left and "z" before "aa" on
    // the right, so the first stored keys "b" < "z" decide. Comparing keys
    // in sorted order would say the opposite ("aa" < "b").
    assert_less(
        &ty,
        &lit("jsonb", "{\"zz\":1,\"b\":0}"),
        &lit("jsonb", "{\"z\":1,\"aa\":9}"),
    );
}

#[test]
fn jsonb_numbers_are_numeric_inside_containers() {
    let ty = KeyType::Jsonb;
    for (a, b) in [
        ("1.0", "1"),
        ("[1.0]", "[1]"),
        ("[1, 2.50]", "[1.0,2.5]"),
        ("{\"a\":1.0}", "{\"a\":1}"),
        ("{\"a\":[1.00,{\"b\":0.0}]}", "{\"a\":[1,{\"b\":0}]}"),
    ] {
        let a = lit("jsonb", &a.replace(' ', ""));
        let b = lit("jsonb", &b.replace(' ', ""));
        assert_same(&ty, &a, &b);
    }
    assert_less(&ty, &lit("jsonb", "[1.0]"), &lit("jsonb", "[1.5]"));
}

// ---------------------------------------------------------------------------
// §7 arrays
// ---------------------------------------------------------------------------

#[test]
fn int4_array_order_nulls_and_lower_bounds() {
    // The spec's examples: {1,2} < {1,2,3} < {1,3}; {1,NULL} > {1,5};
    // {2,1} < {NULL}; [0:1]={1,2} < {1,2}; empty first.
    chain(
        "array",
        &[
            &["{}"],
            &["{-2147483648}"],
            &["{-1}"],
            &["{-1,NULL}"],
            &["{0}"],
            &["{0,NULL}"],
            &["{1}", "[1:1]={1}"],
            &["{1,0}"],
            &["{1,1}"],
            &["[-5:-4]={1,2}"],
            &["[0:1]={1,2}"],
            &["{1,2}"],
            &["[2:3]={1,2}"],
            &["{1,2,3}"],
            &["{{1,2,3}}"],
            &["{1,2,3,4}"],
            &["{{1,2,3,4}}"],
            &["[0:1][0:1]={{1,2},{3,4}}"],
            &["{{1,2},{3,4}}", "[1:2][1:2]={{1,2},{3,4}}"],
            &["{{1},{2},{3},{4}}"],
            &["{1,5}"],
            &["{1,NULL}"],
            &["{1,NULL,NULL}"],
            &["{2}"],
            &["{2,1}"],
            &["{NULL}"],
        ],
    );
}

#[test]
fn array_null_elements_sort_after_values_and_equal_each_other() {
    let ty = KeyType::Array(Box::new(KeyType::Int4));
    let a = |e: Vec<Option<i32>>| int4_array(&[(e.len() as i32, 1)], e);
    assert_less(&ty, &a(vec![Some(1), Some(5)]), &a(vec![Some(1), None]));
    assert_less(&ty, &a(vec![Some(i32::MAX)]), &a(vec![None]));
    assert_less(&ty, &a(vec![Some(2), Some(1)]), &a(vec![None]));
    assert_same(&ty, &a(vec![None]), &a(vec![None]));
    assert_same(&ty, &a(vec![Some(1), None]), &a(vec![Some(1), None]));
    // More elements after an equal prefix: NULL = NULL, then fewer first.
    assert_less(&ty, &a(vec![None]), &a(vec![None, None]));
    // Lower bound is the last tie-break, after dimensions and lengths.
    assert_less(
        &ty,
        &int4_array(&[(2, 0)], vec![Some(1), Some(2)]),
        &int4_array(&[(2, 1)], vec![Some(1), Some(2)]),
    );
    assert_less(
        &ty,
        &int4_array(&[(2, i32::MIN)], vec![Some(1), Some(2)]),
        &int4_array(&[(2, 0)], vec![Some(1), Some(2)]),
    );
    // Fewer dimensions first: [4] < [1,4] for the same four elements.
    assert_less(
        &ty,
        &int4_array(&[(4, 1)], vec![Some(1), Some(2), Some(3), Some(4)]),
        &int4_array(&[(1, 1), (4, 1)], vec![Some(1), Some(2), Some(3), Some(4)]),
    );
}

#[test]
fn array_elements_use_the_element_types_rules() {
    // text[]: NULL after non-NULL, element order bytewise.
    let ty = KeyType::Array(Box::new(KeyType::Text(Collation::C)));
    assert_less(
        &ty,
        &text_array(&[Some("a"), Some("zzz")]),
        &text_array(&[Some("a"), None]),
    );
    assert_less(
        &ty,
        &text_array(&[Some("a")]),
        &text_array(&[Some("a"), Some("")]),
    );
    assert_less(
        &ty,
        &text_array(&[Some("a"), Some("b")]),
        &text_array(&[Some("ab")]),
    );
    assert_same(
        &ty,
        &text_array(&[None, Some("x")]),
        &text_array(&[None, Some("x")]),
    );
    // float8[]: -0 = 0 and NaN = NaN element-wise; NaN above Infinity.
    let fty = KeyType::Array(Box::new(KeyType::Float8));
    let f = |xs: &[f64]| {
        Value::Array(Array::from_elems(
            xs.iter().map(|&x| Some(Value::Float8(x))).collect(),
        ))
    };
    assert_same(&fty, &f(&[-0.0, f64::NAN]), &f(&[0.0, -f64::NAN]));
    assert_less(&fty, &f(&[f64::INFINITY]), &f(&[f64::NAN]));
    // numeric[]: scale-insensitive.
    let nty = KeyType::Array(Box::new(KeyType::Numeric));
    let n = |s: &str| Value::Array(Array::from_elems(vec![Some(lit("numeric", s))]));
    assert_same(&nty, &n("1.0"), &n("1.00"));
    assert_less(&nty, &n("1.00"), &n("1.5"));
    // interval[]: '1 mon' = '30 days'.
    let ity = KeyType::Array(Box::new(KeyType::Interval));
    let i = |s: &str| Value::Array(Array::from_elems(vec![Some(lit("interval", s))]));
    assert_same(&ity, &i("1 mon"), &i("30 days"));
    // jsonb[]: each element is a whole jsonb datum, so rule 6 still applies
    // to it ([] below null).
    let jty = KeyType::Array(Box::new(KeyType::Jsonb));
    let j = |s: &str| Value::Array(Array::from_elems(vec![Some(lit("jsonb", s))]));
    assert_same(&jty, &j("1"), &j("1.0"));
    assert_less(&jty, &j("[]"), &j("null"));
}

// ---------------------------------------------------------------------------
// §2 NULLs, direction, composed keys
// ---------------------------------------------------------------------------

fn col(ty: KeyType, dir: Direction, nulls: Nulls) -> KeyColumn {
    KeyColumn { ty, dir, nulls }
}

#[test]
fn compare_key_places_nulls_and_applies_direction() {
    let i = |x: i32| Some(Value::Int4(x));
    let asc_last = [KeyColumn::asc(KeyType::Int4)];
    let asc_first = [KeyColumn::asc(KeyType::Int4).with_nulls(Nulls::First)];
    let desc_first = [KeyColumn::desc(KeyType::Int4)];
    let desc_last = [KeyColumn::desc(KeyType::Int4).with_nulls(Nulls::Last)];
    // NULL = NULL for ordering and grouping, in every placement.
    for cols in [&asc_last, &asc_first, &desc_first, &desc_last] {
        assert_eq!(compare_key(cols, &[None], &[None]), Ordering::Equal);
        assert_eq!(compare_key(cols, &[i(7)], &[i(7)]), Ordering::Equal);
    }
    // ASC NULLS LAST (the PostgreSQL default) puts NULL above everything.
    assert_eq!(
        compare_key(&asc_last, &[None], &[i(i32::MAX)]),
        Ordering::Greater
    );
    assert_eq!(compare_key(&asc_last, &[i(1)], &[i(2)]), Ordering::Less);
    // NULLS FIRST puts it below.
    assert_eq!(
        compare_key(&asc_first, &[None], &[i(i32::MIN)]),
        Ordering::Less
    );
    assert_eq!(compare_key(&asc_first, &[i(2)], &[None]), Ordering::Greater);
    // DESC reverses values; NULL placement stays the column's.
    assert_eq!(
        compare_key(&desc_first, &[i(1)], &[i(2)]),
        Ordering::Greater
    );
    assert_eq!(
        compare_key(&desc_first, &[None], &[i(i32::MAX)]),
        Ordering::Less
    );
    assert_eq!(
        compare_key(&desc_last, &[None], &[i(i32::MIN)]),
        Ordering::Greater
    );
    assert_eq!(compare_key(&desc_last, &[i(1)], &[i(2)]), Ordering::Greater);
}

#[test]
fn compare_key_is_lexicographic_over_mixed_columns() {
    let cols = [
        col(KeyType::Int4, Direction::Asc, Nulls::Last),
        col(KeyType::Text(Collation::C), Direction::Desc, Nulls::First),
        col(KeyType::Float8, Direction::Asc, Nulls::First),
    ];
    let k = |a: Option<i32>, b: Option<&str>, c: Option<f64>| {
        vec![
            a.map(Value::Int4),
            b.map(|s| Value::Text(s.to_string())),
            c.map(Value::Float8),
        ]
    };
    // The first column decides when it differs.
    assert_eq!(
        compare_key(
            &cols,
            &k(Some(1), Some("z"), Some(9.0)),
            &k(Some(2), Some("a"), Some(0.0))
        ),
        Ordering::Less
    );
    // Equal first column: DESC text reverses ("b" before "a").
    assert_eq!(
        compare_key(
            &cols,
            &k(Some(1), Some("b"), None),
            &k(Some(1), Some("a"), None)
        ),
        Ordering::Less
    );
    // Equal first two columns: float8 ASC NULLS FIRST; -0 = 0, NaN = NaN.
    assert_eq!(
        compare_key(
            &cols,
            &k(Some(1), Some("a"), None),
            &k(Some(1), Some("a"), Some(f64::NEG_INFINITY))
        ),
        Ordering::Less
    );
    assert_eq!(
        compare_key(
            &cols,
            &k(Some(1), Some("a"), Some(-0.0)),
            &k(Some(1), Some("a"), Some(0.0))
        ),
        Ordering::Equal
    );
    assert_eq!(
        compare_key(&cols, &k(None, None, None), &k(None, None, None)),
        Ordering::Equal
    );
    // NULL first column (NULLS LAST) is above any value; DESC text NULLS FIRST
    // puts a NULL text below any text.
    assert_eq!(
        compare_key(
            &cols,
            &k(None, Some("a"), None),
            &k(Some(i32::MAX), Some("a"), None)
        ),
        Ordering::Greater
    );
    assert_eq!(
        compare_key(&cols, &k(Some(1), None, None), &k(Some(1), Some(""), None)),
        Ordering::Less
    );
}

// ---------------------------------------------------------------------------
// in_bounds
// ---------------------------------------------------------------------------

#[test]
fn in_bounds_inclusive_exclusive_unbounded() {
    let ty = KeyType::Int4;
    let v = |x: i32| Value::Int4(x);
    let (lo, hi) = (v(10), v(20));
    let cases = [
        (v(9), false),
        (v(10), true),
        (v(15), true),
        (v(20), true),
        (v(21), false),
    ];
    for (x, want) in cases {
        assert_eq!(
            in_bounds(&ty, &x, Bound::Included(&lo), Bound::Included(&hi)),
            want,
            "{x:?} in [10,20]"
        );
    }
    assert!(!in_bounds(
        &ty,
        &v(10),
        Bound::Excluded(&lo),
        Bound::Included(&hi)
    ));
    assert!(in_bounds(
        &ty,
        &v(11),
        Bound::Excluded(&lo),
        Bound::Included(&hi)
    ));
    assert!(!in_bounds(
        &ty,
        &v(20),
        Bound::Included(&lo),
        Bound::Excluded(&hi)
    ));
    assert!(in_bounds(
        &ty,
        &v(19),
        Bound::Included(&lo),
        Bound::Excluded(&hi)
    ));
    assert!(in_bounds(
        &ty,
        &v(i32::MIN),
        Bound::Unbounded,
        Bound::Included(&hi)
    ));
    assert!(!in_bounds(
        &ty,
        &v(i32::MIN),
        Bound::Included(&lo),
        Bound::Unbounded
    ));
    assert!(in_bounds(
        &ty,
        &v(i32::MAX),
        Bound::Included(&lo),
        Bound::Unbounded
    ));
    assert!(in_bounds(&ty, &v(0), Bound::Unbounded, Bound::Unbounded));
    // Crossing bounds admit nothing, equal exclusive bounds admit nothing.
    assert!(!in_bounds(
        &ty,
        &v(15),
        Bound::Included(&hi),
        Bound::Included(&lo)
    ));
    assert!(!in_bounds(
        &ty,
        &v(10),
        Bound::Excluded(&lo),
        Bound::Excluded(&lo)
    ));
    assert!(in_bounds(
        &ty,
        &v(10),
        Bound::Included(&lo),
        Bound::Included(&lo)
    ));
}

#[test]
fn in_bounds_uses_the_type_rules_not_the_representation() {
    // float8: NaN is above +Inf, so (Inf, ..) excludes Inf and includes NaN;
    // -0 and 0 are one point.
    let f = KeyType::Float8;
    let (inf, nan) = (Value::Float8(f64::INFINITY), Value::Float8(f64::NAN));
    assert!(in_bounds(&f, &nan, Bound::Excluded(&inf), Bound::Unbounded));
    assert!(!in_bounds(
        &f,
        &inf,
        Bound::Excluded(&inf),
        Bound::Unbounded
    ));
    assert!(in_bounds(
        &f,
        &Value::Float8(-0.0),
        Bound::Included(&Value::Float8(0.0)),
        Bound::Included(&Value::Float8(0.0))
    ));
    assert!(!in_bounds(
        &f,
        &Value::Float8(-0.0),
        Bound::Excluded(&Value::Float8(0.0)),
        Bound::Unbounded
    ));
    // numeric: 1.00 is inside [1, 1].
    let n = KeyType::Numeric;
    let one = lit("numeric", "1");
    assert!(in_bounds(
        &n,
        &lit("numeric", "1.00"),
        Bound::Included(&one),
        Bound::Included(&one)
    ));
    // interval: '30 days' is inside ['1 mon', '1 mon'].
    let i = KeyType::Interval;
    let mon = lit("interval", "1 mon");
    assert!(in_bounds(
        &i,
        &lit("interval", "30 days"),
        Bound::Included(&mon),
        Bound::Included(&mon)
    ));
    // text: prefix logic. 'ab' is in ['a', 'b'), 'a' is not in ('a', 'b'),
    // 'b' is not in ['a', 'b').
    let t = KeyType::Text(Collation::C);
    let (a, b) = (lit("text", "'a'"), lit("text", "'b'"));
    assert!(in_bounds(
        &t,
        &lit("text", "'ab'"),
        Bound::Included(&a),
        Bound::Excluded(&b)
    ));
    assert!(!in_bounds(&t, &a, Bound::Excluded(&a), Bound::Excluded(&b)));
    assert!(!in_bounds(&t, &b, Bound::Included(&a), Bound::Excluded(&b)));
    // date infinities are ordinary endpoints.
    let d = KeyType::Date;
    let ninf = Value::Date(i32::MIN);
    assert!(in_bounds(
        &d,
        &ninf,
        Bound::Included(&ninf),
        Bound::Unbounded
    ));
    assert!(!in_bounds(
        &d,
        &ninf,
        Bound::Excluded(&ninf),
        Bound::Unbounded
    ));
    // jsonb rule 6 in a scan: [] is below null, so null is not in (.., []].
    let j = KeyType::Jsonb;
    let (empty, null) = (lit("jsonb", "[]"), lit("jsonb", "null"));
    assert!(in_bounds(
        &j,
        &empty,
        Bound::Unbounded,
        Bound::Excluded(&null)
    ));
    assert!(!in_bounds(
        &j,
        &null,
        Bound::Unbounded,
        Bound::Included(&empty)
    ));
}

// ---------------------------------------------------------------------------
// Invalid values: errors from try_*, fixed fallbacks from the shims
// ---------------------------------------------------------------------------

#[test]
fn invalid_values_error_and_never_panic() {
    let ty = KeyType::Time;
    let bad = Value::Time(-1);
    let good = Value::Time(0);
    assert!(matches!(
        try_compare(&ty, &bad, &good),
        Err(CodecError::InvalidValue(_))
    ));
    assert!(matches!(
        try_eq(&ty, &good, &bad),
        Err(CodecError::InvalidValue(_))
    ));
    assert!(matches!(
        try_hash(&ty, &bad),
        Err(CodecError::InvalidValue(_))
    ));
    assert!(try_in_bounds(&ty, &bad, Bound::Unbounded, Bound::Unbounded).is_err());
    assert!(try_in_bounds(&ty, &good, Bound::Included(&bad), Bound::Unbounded).is_err());
    // Fallbacks: invalid sorts last, never equal, hashes to 0, in no range.
    assert_eq!(compare(&ty, &bad, &good), Ordering::Greater);
    assert_eq!(compare(&ty, &good, &bad), Ordering::Less);
    assert_eq!(compare(&ty, &bad, &Value::Time(-2)), Ordering::Equal);
    assert!(!eq(&ty, &bad, &bad));
    assert_eq!(hash(&ty, &bad), 0);
    assert!(!in_bounds(&ty, &bad, Bound::Unbounded, Bound::Unbounded));
    assert!(!in_bounds(
        &ty,
        &good,
        Bound::Included(&bad),
        Bound::Unbounded
    ));
    // Value of the wrong type for the declared type.
    assert_eq!(
        try_compare(&KeyType::Int4, &Value::Int8(1), &Value::Int8(1)),
        Err(CodecError::TypeMismatch)
    );
    assert_eq!(
        try_eq(&KeyType::Int4, &Value::Int4(1), &Value::Int8(1)),
        Err(CodecError::TypeMismatch)
    );
    // Numeric digits outside 0..=9 and exponents outside i32.
    let ty = KeyType::Numeric;
    assert!(try_compare(&ty, &dec(false, &[10], 0), &dec(false, &[1], 0)).is_err());
    assert!(try_compare(&ty, &dec(false, &[1], i32::MIN), &dec(false, &[1], 0)).is_err());
    // jsonb numbers must be finite.
    let nan = Value::Jsonb(Jsonb::Number(Numeric::NaN));
    assert!(try_compare(&KeyType::Jsonb, &nan, &nan).is_err());
    // A bad array shape (dims do not match the element count).
    let aty = KeyType::Array(Box::new(KeyType::Int4));
    let bad_arr = int4_array(&[(3, 1)], vec![Some(1)]);
    assert!(try_hash(&aty, &bad_arr).is_err());
    // compare_key: arity mismatch is an error from the codec, sorted last.
    let cols = [KeyColumn::asc(KeyType::Int4)];
    assert_eq!(
        compare_key(
            &cols,
            &[Some(Value::Int4(1)), None],
            &[Some(Value::Int4(1))]
        ),
        Ordering::Greater
    );
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

/// One value per `KeyType` (arrays of two element types).
fn samples() -> Vec<(KeyType, Value)> {
    vec![
        (KeyType::Bool, Value::Bool(true)),
        (KeyType::Int2, Value::Int2(-2)),
        (KeyType::Int4, Value::Int4(4)),
        (KeyType::Int8, Value::Int8(8)),
        (KeyType::Float4, Value::Float4(1.5)),
        (KeyType::Float8, Value::Float8(2.5)),
        (KeyType::Numeric, lit("numeric", "3.50")),
        (KeyType::Text(Collation::C), Value::Text("t".into())),
        (KeyType::Bytea, Value::Bytea(vec![0, 1])),
        (KeyType::Date, Value::Date(5)),
        (KeyType::Time, Value::Time(6)),
        (KeyType::Timestamp, Value::Timestamp(7)),
        (KeyType::TimestampTz, Value::TimestampTz(8)),
        (KeyType::Interval, lit("interval", "1 mon")),
        (KeyType::Uuid, Value::Uuid([9; 16])),
        (KeyType::Jsonb, lit("jsonb", "{\"a\":[1]}")),
        (
            KeyType::Array(Box::new(KeyType::Int4)),
            int4_array(&[(2, 1)], vec![Some(1), None]),
        ),
        (
            KeyType::Array(Box::new(KeyType::Text(Collation::C))),
            text_array(&[Some("x"), None]),
        ),
    ]
}

#[test]
fn registry_has_a_complete_row_for_every_type() {
    assert_eq!(TYPE_OPS.len(), TypeKind::COUNT);
    for (i, k) in TypeKind::ALL.iter().enumerate() {
        assert_eq!(k.index(), i);
        assert_eq!(TYPE_OPS[i].kind, *k);
        assert_eq!(row_of(*k).kind, *k);
    }
    let mut seen = [false; TypeKind::COUNT];
    for (ty, v) in samples() {
        let kind = kind_of(&ty);
        seen[kind.index()] = true;
        let row = ops(&ty);
        assert_eq!(row.kind, kind, "{ty:?}");
        // Every slot is populated and usable on a value of its own type.
        assert_eq!((row.compare)(&ty, &v, &v), Ok(Ordering::Equal), "{ty:?}");
        assert_eq!((row.eq)(&ty, &v, &v), Ok(true), "{ty:?}");
        assert_eq!((row.hash)(&ty, &v), try_hash(&ty, &v), "{ty:?}");
        assert!(try_hash(&ty, &v).is_ok());
    }
    assert!(seen.iter().all(|s| *s), "samples() must cover every kind");
}

#[test]
fn registry_slots_serve_only_their_own_type() {
    // A row wired to another type's kernel would silently compare the wrong
    // thing; each slot instead refuses any declared type but its own.
    for (own_ty, _) in samples() {
        let row = ops(&own_ty);
        for (other_ty, other_v) in samples() {
            if kind_of(&other_ty) == kind_of(&own_ty) {
                continue;
            }
            assert_eq!(
                (row.compare)(&other_ty, &other_v, &other_v),
                Err(CodecError::TypeMismatch),
                "{:?} compare slot accepted {other_ty:?}",
                row.kind
            );
            assert_eq!(
                (row.eq)(&other_ty, &other_v, &other_v),
                Err(CodecError::TypeMismatch),
                "{:?} eq slot accepted {other_ty:?}",
                row.kind
            );
            assert_eq!(
                (row.hash)(&other_ty, &other_v),
                Err(CodecError::TypeMismatch),
                "{:?} hash slot accepted {other_ty:?}",
                row.kind
            );
        }
    }
}

#[test]
fn registry_slots_agree_with_the_codec() {
    // hash is exactly the codec's hash; the kind a type reports is stable
    // across element types and collations.
    for (ty, v) in samples() {
        assert_eq!(
            try_hash(&ty, &v),
            nucleus_codec::hash_value(&ty, &v),
            "{ty:?}"
        );
    }
    assert_eq!(
        kind_of(&KeyType::Array(Box::new(KeyType::Int4))),
        kind_of(&KeyType::Array(Box::new(KeyType::Jsonb)))
    );
    assert_ne!(
        kind_of(&KeyType::Array(Box::new(KeyType::Int4))),
        kind_of(&KeyType::Int4)
    );
}

#[test]
fn op_oid_space_names_the_three_populated_slots() {
    assert_eq!(registry::POPULATED_OPS, [OP_BTREE_CMP, OP_EQ, OP_HASH]);
    let mut oids = registry::POPULATED_OPS.to_vec();
    oids.sort_unstable();
    oids.dedup();
    assert_eq!(oids.len(), 3);
}
