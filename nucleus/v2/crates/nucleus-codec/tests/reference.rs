//! Independent reference comparator and value strategies (card C-K2 work
//! items 1-2). Included by `tests/properties.rs` as `mod reference;`.
//!
//! `sql_cmp` is written directly from the PostgreSQL rules C-Q3s cites per
//! type: float total order with NaN greatest and `-0 == 0`, numeric by value
//! with NaN greatest, interval normalisation, bytea/text bytewise, the jsonb
//! order and the array order. It never calls the encoder: no `encode_key`,
//! no `hash_value`, no encoder internals.
//!
//! The strategies are biased to the same edge values as the golden corpus:
//! infinities and sentinels, NaN payloads, signed zeros, empty and prefix
//! strings, scale variants, NULL elements, dimension and lower-bound edges.

#![allow(dead_code)]

use std::cmp::Ordering;

use nucleus_codec::{
    Array, ArrayDim, Collation, Decimal, Direction, Interval, Jsonb, KeyColumn, KeyType, Nulls,
    Numeric, Value,
};
use proptest::collection::vec;
use proptest::prelude::*;

// ---------------------------------------------------------------------------
// Reference comparison (C-Q3s, from the PostgreSQL rules)
// ---------------------------------------------------------------------------

/// PostgreSQL btree comparison for two values of one declared type: the
/// right-hand side of P-ORDER.
pub fn sql_cmp(ty: &KeyType, a: &Value, b: &Value) -> Ordering {
    match (ty, a, b) {
        (KeyType::Bool, Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (KeyType::Int2, Value::Int2(x), Value::Int2(y)) => x.cmp(y),
        (KeyType::Int4, Value::Int4(x), Value::Int4(y)) => x.cmp(y),
        (KeyType::Int8, Value::Int8(x), Value::Int8(y)) => x.cmp(y),
        (KeyType::Float4, Value::Float4(x), Value::Float4(y)) => float_total_cmp(*x, *y),
        (KeyType::Float8, Value::Float8(x), Value::Float8(y)) => float_total_cmp(*x, *y),
        (KeyType::Numeric, Value::Numeric(x), Value::Numeric(y)) => numeric_cmp(x, y),
        // Collation C: plain bytewise order on the UTF-8 bytes (§5).
        (KeyType::Text(_), Value::Text(x), Value::Text(y)) => x.as_bytes().cmp(y.as_bytes()),
        (KeyType::Bytea, Value::Bytea(x), Value::Bytea(y)) => x.cmp(y),
        (KeyType::Date, Value::Date(x), Value::Date(y)) => x.cmp(y),
        (KeyType::Time, Value::Time(x), Value::Time(y)) => x.cmp(y),
        (KeyType::Timestamp, Value::Timestamp(x), Value::Timestamp(y)) => x.cmp(y),
        (KeyType::TimestampTz, Value::TimestampTz(x), Value::TimestampTz(y)) => x.cmp(y),
        (KeyType::Interval, Value::Interval(x), Value::Interval(y)) => {
            interval_span(x).cmp(&interval_span(y))
        }
        (KeyType::Uuid, Value::Uuid(x), Value::Uuid(y)) => x.cmp(y),
        (KeyType::Jsonb, Value::Jsonb(x), Value::Jsonb(y)) => jsonb_top_cmp(x, y),
        (KeyType::Array(elem), Value::Array(x), Value::Array(y)) => array_cmp(elem, x, y),
        _ => panic!("reference comparator: value does not match key type {ty:?}"),
    }
}

/// One key column's order (§2): NULL placement around the value order,
/// reversed for DESC.
pub fn column_cmp(col: &KeyColumn, a: &Option<Value>, b: &Option<Value>) -> Ordering {
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => match col.nulls {
            Nulls::First => Ordering::Less,
            Nulls::Last => Ordering::Greater,
        },
        (Some(_), None) => column_cmp(col, b, a).reverse(),
        (Some(x), Some(y)) => {
            let o = sql_cmp(&col.ty, x, y);
            if col.dir == Direction::Desc {
                o.reverse()
            } else {
                o
            }
        }
    }
}

/// §3: byte order is the lexicographic order of the columns.
pub fn key_cmp(cols: &[KeyColumn], a: &[Option<Value>], b: &[Option<Value>]) -> Ordering {
    let mut o = Ordering::Equal;
    for (c, (x, y)) in cols.iter().zip(a.iter().zip(b.iter())) {
        o = column_cmp(c, x, y);
        if o != Ordering::Equal {
            return o;
        }
    }
    o
}

/// §4.1 `float8_cmp_internal` / `float4_cmp_internal`: `-Infinity < negatives
/// < -0 = 0 < positives < +Infinity < NaN`, `NaN = NaN`.
trait FloatTotal: PartialOrd + Copy {
    fn is_nan_total(&self) -> bool;
}
impl FloatTotal for f32 {
    fn is_nan_total(&self) -> bool {
        self.is_nan()
    }
}
impl FloatTotal for f64 {
    fn is_nan_total(&self) -> bool {
        self.is_nan()
    }
}

fn float_total_cmp<F: FloatTotal>(x: F, y: F) -> Ordering {
    match (x.is_nan_total(), y.is_nan_total()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        // Total on non-NaN; -0 == 0 by IEEE comparison.
        (false, false) => x.partial_cmp(&y).unwrap_or(Ordering::Equal),
    }
}

/// §4.2 `cmp_numerics`: `-Infinity < finite by value < +Infinity < NaN`.
fn numeric_cmp(a: &Numeric, b: &Numeric) -> Ordering {
    match (a, b) {
        (Numeric::NaN, Numeric::NaN) => Ordering::Equal,
        (Numeric::NaN, _) => Ordering::Greater,
        (_, Numeric::NaN) => Ordering::Less,
        (Numeric::PosInf, Numeric::PosInf) => Ordering::Equal,
        (Numeric::PosInf, _) => Ordering::Greater,
        (_, Numeric::PosInf) => Ordering::Less,
        (Numeric::NegInf, Numeric::NegInf) => Ordering::Equal,
        (Numeric::NegInf, _) => Ordering::Less,
        (_, Numeric::NegInf) => Ordering::Greater,
        (Numeric::Finite(x), Numeric::Finite(y)) => decimal_cmp(x, y),
    }
}

/// Finite numerics by mathematical value: `1 = 1.0 = 1.00`, `0 = -0 = 0.00`;
/// display scale is not part of equality.
fn decimal_cmp(a: &Decimal, b: &Decimal) -> Ordering {
    let (az, bz) = (is_zero(a), is_zero(b));
    match (az, bz) {
        (true, true) => return Ordering::Equal,
        (true, false) => {
            return if b.negative {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (false, true) => {
            return if a.negative {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (false, false) => {}
    }
    if a.negative != b.negative {
        return if a.negative {
            Ordering::Less
        } else {
            Ordering::Greater
        };
    }
    let m = magnitude_cmp(a, b);
    if a.negative {
        m.reverse()
    } else {
        m
    }
}

fn is_zero(d: &Decimal) -> bool {
    d.digits.iter().all(|&x| x == 0)
}

/// Compare magnitudes as `0.d1 d2 ... dn * 10^E` with `d1 != 0`, `dn != 0`
/// (the §4.2 normal form, unique per value).
fn magnitude_cmp(a: &Decimal, b: &Decimal) -> Ordering {
    let (sa, ea) = significant(a);
    let (sb, eb) = significant(b);
    ea.cmp(&eb).then_with(|| {
        let n = sa.len().min(sb.len());
        // Equal prefix: the longer significant digit list is larger, because
        // every digit past the shorter list is non-zero.
        sa[..n].cmp(&sb[..n]).then_with(|| sa.len().cmp(&sb.len()))
    })
}

/// Significant digits (leading and trailing zeros stripped) and the decimal
/// exponent: the value is `0.d1..dn * 10^E`.
fn significant(d: &Decimal) -> (&[u8], i64) {
    let start = d.digits.iter().position(|&x| x != 0).unwrap_or(0);
    let end = d
        .digits
        .iter()
        .rposition(|&x| x != 0)
        .map_or(start, |e| e + 1);
    let exp = d.digits.len() as i64 - start as i64 - i64::from(d.scale);
    (&d.digits[start..end], exp)
}

/// §4.3 `interval_cmp_value`: the 128-bit total `(months*30 + days) days +
/// time`, in microseconds. `'1 mon' = '30 days'`, `'1 day' = '24 hours'`.
fn interval_span(i: &Interval) -> i128 {
    (i128::from(i.months) * 30 + i128::from(i.days)) * i128::from(Interval::USECS_PER_DAY)
        + i128::from(i.micros)
}

/// §6 `compareJsonbContainers` at top level, with rule 6: the top-level
/// empty array sorts below every top-level scalar, null included.
fn jsonb_top_cmp(a: &Jsonb, b: &Jsonb) -> Ordering {
    top_class(a)
        .cmp(&top_class(b))
        .then_with(|| jsonb_nested_cmp(a, b))
}

/// Top-level class (§6 rules 1 and 6): `[]` < scalars < non-empty arrays <
/// objects. Counts never compete across classes: a raw scalar is a
/// one-element pseudo-array, so its count never beats a non-empty array's.
fn top_class(j: &Jsonb) -> u8 {
    match j {
        Jsonb::Array(a) if a.is_empty() => 0,
        Jsonb::Object(_) => 3,
        Jsonb::Array(_) => 2,
        _ => 1,
    }
}

/// §6 rules 1-5 for two jsonb values of the same class (or any two nested
/// values, where rule 6 does not apply).
fn jsonb_nested_cmp(a: &Jsonb, b: &Jsonb) -> Ordering {
    nested_class(a)
        .cmp(&nested_class(b))
        .then_with(|| jsonb_same_class_cmp(a, b))
}

/// §6 rule 1: Object > Array > Boolean > Number > String > Null.
fn nested_class(j: &Jsonb) -> u8 {
    match j {
        Jsonb::Object(_) => 5,
        Jsonb::Array(_) => 4,
        Jsonb::Bool(_) => 3,
        Jsonb::Number(_) => 2,
        Jsonb::String(_) => 1,
        Jsonb::Null => 0,
    }
}

fn jsonb_same_class_cmp(a: &Jsonb, b: &Jsonb) -> Ordering {
    match (a, b) {
        (Jsonb::Object(x), Jsonb::Object(y)) => jsonb_object_cmp(x, y),
        (Jsonb::Array(x), Jsonb::Array(y)) => jsonb_array_cmp(x, y),
        (Jsonb::Bool(x), Jsonb::Bool(y)) => x.cmp(y),
        (Jsonb::Number(x), Jsonb::Number(y)) => numeric_cmp(x, y),
        (Jsonb::String(x), Jsonb::String(y)) => x.as_bytes().cmp(y.as_bytes()),
        (Jsonb::Null, Jsonb::Null) => Ordering::Equal,
        _ => Ordering::Equal, // different classes: decided by the class rank
    }
}

/// §6 rules 2 and 4: element count first, then element by element.
fn jsonb_array_cmp(x: &[Jsonb], y: &[Jsonb]) -> Ordering {
    x.len().cmp(&y.len()).then_with(|| {
        for (a, b) in x.iter().zip(y.iter()) {
            let o = jsonb_nested_cmp(a, b);
            if o != Ordering::Equal {
                return o;
            }
        }
        Ordering::Equal
    })
}

/// §6 rules 2, 3 and 7: pair count first, then key 1, value 1, key 2, ...
/// in storage order (key length, then bytewise) after duplicate removal
/// (last value wins).
fn jsonb_object_cmp(x: &[(String, Jsonb)], y: &[(String, Jsonb)]) -> Ordering {
    let cx = canon_pairs(x);
    let cy = canon_pairs(y);
    cx.len().cmp(&cy.len()).then_with(|| {
        for ((ka, va), (kb, vb)) in cx.iter().zip(cy.iter()) {
            let o = ka.as_bytes().cmp(kb.as_bytes());
            if o != Ordering::Equal {
                return o;
            }
            let o = jsonb_nested_cmp(va, vb);
            if o != Ordering::Equal {
                return o;
            }
        }
        Ordering::Equal
    })
}

/// jsonb input semantics (§6 rule 7): keys sorted by (length, bytes),
/// duplicates collapsed keeping the last value.
fn canon_pairs<'a>(pairs: &'a [(String, Jsonb)]) -> Vec<(&'a str, &'a Jsonb)> {
    let mut sorted: Vec<&'a (String, Jsonb)> = pairs.iter().collect();
    sorted.sort_by(|a, b| {
        a.0.len()
            .cmp(&b.0.len())
            .then_with(|| a.0.as_bytes().cmp(b.0.as_bytes()))
    });
    let mut out: Vec<(&'a str, &'a Jsonb)> = Vec::with_capacity(sorted.len());
    for p in sorted {
        match out.last_mut() {
            Some(last) if last.0 == p.0.as_str() => *last = (p.0.as_str(), &p.1),
            _ => out.push((p.0.as_str(), &p.1)),
        }
    }
    out
}

/// §7 `array_cmp`: elements pairwise in row-major order up to the shorter
/// count (NULL elements after non-NULL, fixed regardless of the column's
/// ASC/DESC or NULLS placement), then fewer elements, fewer dimensions,
/// each dimension length, each lower bound.
fn array_cmp(elem: &KeyType, x: &Array, y: &Array) -> Ordering {
    for (ex, ey) in x.elems.iter().zip(y.elems.iter()) {
        let o = match (ex, ey) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Greater,
            (Some(_), None) => Ordering::Less,
            (Some(vx), Some(vy)) => sql_cmp(elem, vx, vy),
        };
        if o != Ordering::Equal {
            return o;
        }
    }
    x.elems
        .len()
        .cmp(&y.elems.len())
        .then_with(|| x.dims.len().cmp(&y.dims.len()))
        .then_with(|| dims_field(&x.dims, &y.dims, |d| d.len))
        .then_with(|| dims_field(&x.dims, &y.dims, |d| d.lower))
}

fn dims_field(x: &[ArrayDim], y: &[ArrayDim], f: impl Fn(&ArrayDim) -> i32) -> Ordering {
    for (a, b) in x.iter().zip(y.iter()) {
        let o = f(a).cmp(&f(b));
        if o != Ordering::Equal {
            return o;
        }
    }
    Ordering::Equal
}

// ---------------------------------------------------------------------------
// Strategies (biased to the golden corpus edge values)
// ---------------------------------------------------------------------------

pub fn scalar_key_type() -> BoxedStrategy<KeyType> {
    prop_oneof![
        1 => Just(KeyType::Bool),
        1 => Just(KeyType::Int2),
        1 => Just(KeyType::Int4),
        1 => Just(KeyType::Int8),
        2 => Just(KeyType::Float4),
        2 => Just(KeyType::Float8),
        3 => Just(KeyType::Numeric),
        2 => Just(KeyType::Text(Collation::C)),
        1 => Just(KeyType::Bytea),
        1 => Just(KeyType::Date),
        1 => Just(KeyType::Time),
        1 => Just(KeyType::Timestamp),
        1 => Just(KeyType::TimestampTz),
        2 => Just(KeyType::Interval),
        1 => Just(KeyType::Uuid),
        2 => Just(KeyType::Jsonb),
    ]
    .boxed()
}

/// Every key type; arrays of a scalar element type included, arrays of
/// arrays excluded (the encoder rejects them).
pub fn key_type() -> BoxedStrategy<KeyType> {
    prop_oneof![
        5 => scalar_key_type(),
        1 => scalar_key_type().prop_map(|t| KeyType::Array(Box::new(t))),
    ]
    .boxed()
}

fn biased_i16() -> BoxedStrategy<i16> {
    prop_oneof![
        1 => Just(i16::MIN),
        1 => Just(i16::MAX),
        1 => Just(0),
        1 => Just(1),
        1 => Just(-1),
        6 => any::<i16>(),
    ]
    .boxed()
}

fn biased_i32() -> BoxedStrategy<i32> {
    prop_oneof![
        1 => Just(i32::MIN),
        1 => Just(i32::MAX),
        1 => Just(0),
        1 => Just(1),
        1 => Just(-1),
        6 => any::<i32>(),
    ]
    .boxed()
}

fn biased_i64() -> BoxedStrategy<i64> {
    prop_oneof![
        1 => Just(i64::MIN),
        1 => Just(i64::MAX),
        1 => Just(0),
        1 => Just(1),
        1 => Just(-1),
        6 => any::<i64>(),
    ]
    .boxed()
}

/// Any bit pattern, biased to the float edge values: both zeros, both
/// infinities, extreme finites, NaN with either sign and any payload.
fn f64_value() -> BoxedStrategy<f64> {
    prop_oneof![
        2 => Just(0.0),
        2 => Just(-0.0),
        1 => Just(f64::INFINITY),
        1 => Just(f64::NEG_INFINITY),
        1 => Just(f64::NAN),
        1 => Just(f64::from_bits(0xFFF8_0000_0000_0000)),
        2 => (0x7FF0_0000_0000_0001u64..=0x7FFF_FFFF_FFFF_FFFF).prop_map(f64::from_bits),
        1 => Just(f64::MIN),
        1 => Just(f64::MAX),
        1 => Just(f64::MIN_POSITIVE),
        10 => any::<u64>().prop_map(f64::from_bits),
    ]
    .boxed()
}

fn f32_value() -> BoxedStrategy<f32> {
    prop_oneof![
        2 => Just(0.0),
        2 => Just(-0.0),
        1 => Just(f32::INFINITY),
        1 => Just(f32::NEG_INFINITY),
        1 => Just(f32::NAN),
        1 => Just(f32::from_bits(0xFFC0_0000)),
        2 => (0x7F80_0001u32..=0x7FFF_FFFF).prop_map(f32::from_bits),
        1 => Just(f32::MIN),
        1 => Just(f32::MAX),
        1 => Just(f32::MIN_POSITIVE),
        10 => any::<u32>().prop_map(f32::from_bits),
    ]
    .boxed()
}

/// Digits with leading and trailing zeros (display scale is not equality),
/// biased to zero and negative zero.
fn decimal() -> BoxedStrategy<Decimal> {
    prop_oneof![
        2 => any::<bool>().prop_map(|negative| Decimal {
            negative,
            digits: vec![0],
            scale: 0,
        }),
        10 => (any::<bool>(), vec(0u8..=9, 1..=12), -4i32..=12).prop_map(
            |(negative, digits, scale)| Decimal {
                negative,
                digits,
                scale,
            }
        ),
    ]
    .boxed()
}

fn numeric(finite_only: bool) -> BoxedStrategy<Numeric> {
    if finite_only {
        decimal().prop_map(Numeric::Finite).boxed()
    } else {
        prop_oneof![
            1 => Just(Numeric::NaN),
            1 => Just(Numeric::PosInf),
            1 => Just(Numeric::NegInf),
            12 => decimal().prop_map(Numeric::Finite),
        ]
        .boxed()
    }
}

fn text_char() -> BoxedStrategy<char> {
    prop_oneof![
        2 => Just('\0'),
        3 => Just('a'),
        2 => Just('b'),
        2 => Just('é'),
        2 => Just('你'),
        4 => any::<char>(),
    ]
    .boxed()
}

fn text() -> BoxedStrategy<String> {
    vec(text_char(), 0..=8)
        .prop_map(|cs| cs.into_iter().collect())
        .boxed()
}

fn bytea() -> BoxedStrategy<Vec<u8>> {
    prop_oneof![
        2 => Just(Vec::new()),
        2 => Just(vec![0]),
        1 => Just(vec![0, 1]),
        1 => Just(vec![0, 0xFF]),
        1 => Just(vec![0xFF; 4]),
        8 => vec(any::<u8>(), 0..=8),
    ]
    .boxed()
}

/// §4 domain: `0 ..= 86_400_000_000` (`24:00:00` included).
fn time_value() -> BoxedStrategy<i64> {
    prop_oneof![
        1 => Just(0),
        1 => Just(86_400_000_000),
        1 => Just(1),
        1 => Just(86_399_999_999),
        6 => 0i64..=86_400_000_000,
    ]
    .boxed()
}

fn uuid() -> BoxedStrategy<[u8; 16]> {
    prop_oneof![
        1 => Just([0u8; 16]),
        1 => Just([0xFFu8; 16]),
        6 => any::<[u8; 16]>(),
    ]
    .boxed()
}

fn interval() -> BoxedStrategy<Interval> {
    prop_oneof![
        1 => Just(Interval::NEG_INFINITY),
        1 => Just(Interval::INFINITY),
        10 => (biased_i32(), biased_i32(), biased_i64()).prop_map(
            |(months, days, micros)| Interval { months, days, micros }
        ),
        2 => Just(Interval { months: 1, days: 0, micros: 0 }),
        2 => Just(Interval { months: 0, days: 30, micros: 0 }),
        2 => Just(Interval { months: 0, days: 1, micros: 0 }),
        2 => Just(Interval { months: 0, days: 0, micros: 86_400_000_000 }),
    ]
    .boxed()
}

fn jsonb_key() -> BoxedStrategy<String> {
    prop_oneof![
        2 => Just(String::new()),
        4 => Just(String::from("a")),
        3 => Just(String::from("b")),
        2 => Just(String::from("c")),
        2 => Just(String::from("aa")),
        2 => Just(String::from("ab")),
        // Short keys over a tiny alphabet: length-first storage order
        // (§6 rule 3) and plain bytewise order disagree often, e.g.
        // {"aa","c"} vs {"b","d"}.
        4 => vec(prop_oneof![Just('a'), Just('b'), Just('c'), Just('d')], 0..=3)
            .prop_map(|cs| cs.into_iter().collect()),
    ]
    .boxed()
}

/// Duplicate keys and any input order allowed: the encoder applies jsonb
/// input semantics, so the strategies must too. Depth-bounded recursion,
/// small containers (counts and nesting drive jsonb order).
fn jsonb(depth: u32) -> BoxedStrategy<Jsonb> {
    if depth == 0 {
        prop_oneof![
            2 => Just(Jsonb::Null),
            2 => any::<bool>().prop_map(Jsonb::Bool),
            4 => numeric(true).prop_map(Jsonb::Number),
            4 => text().prop_map(Jsonb::String),
        ]
        .boxed()
    } else {
        prop_oneof![
            5 => jsonb(depth - 1),
            3 => vec(jsonb(depth - 1), 0..=3).prop_map(Jsonb::Array),
            3 => vec((jsonb_key(), jsonb(depth - 1)), 0..=3).prop_map(Jsonb::Object),
        ]
        .boxed()
    }
}

fn jsonb_value() -> BoxedStrategy<Jsonb> {
    jsonb(3)
}

/// Dimension lists valid for the encoder: 1..=3 dimensions of length 1..=3
/// (an empty array has zero dimensions), lower bounds at the domain edges.
fn array_dims() -> BoxedStrategy<Vec<ArrayDim>> {
    vec(1u32..=3, 0..=3)
        .prop_flat_map(|lens| {
            let lowers: Vec<BoxedStrategy<i32>> = lens
                .iter()
                .map(|&len| {
                    let max = i32::MAX - len as i32 + 1;
                    prop_oneof![
                        1 => Just(i32::MIN),
                        1 => Just(max),
                        1 => Just(0),
                        1 => Just(1),
                        1 => Just(-1),
                        3 => i32::MIN..=max,
                    ]
                    .boxed()
                })
                .collect();
            lowers.prop_map(move |low| {
                lens.iter()
                    .zip(low.iter())
                    .map(|(&l, &lo)| ArrayDim {
                        len: l as i32,
                        lower: lo,
                    })
                    .collect::<Vec<ArrayDim>>()
            })
        })
        .boxed()
}

fn sql_array(elem: KeyType) -> BoxedStrategy<Array> {
    array_dims()
        .prop_flat_map(move |dims| {
            // An empty array has zero dimensions (and vice versa).
            let n: usize = if dims.is_empty() {
                0
            } else {
                dims.iter().map(|d| d.len as usize).product()
            };
            vec(optional_value(elem.clone()), n..=n).prop_map(move |elems| Array {
                dims: dims.clone(),
                elems,
            })
        })
        .boxed()
}

pub fn typed_value(ty: KeyType) -> BoxedStrategy<Value> {
    match ty {
        KeyType::Bool => any::<bool>().prop_map(Value::Bool).boxed(),
        KeyType::Int2 => biased_i16().prop_map(Value::Int2).boxed(),
        KeyType::Int4 => biased_i32().prop_map(Value::Int4).boxed(),
        KeyType::Int8 => biased_i64().prop_map(Value::Int8).boxed(),
        KeyType::Float4 => f32_value().prop_map(Value::Float4).boxed(),
        KeyType::Float8 => f64_value().prop_map(Value::Float8).boxed(),
        KeyType::Numeric => numeric(false).prop_map(Value::Numeric).boxed(),
        KeyType::Text(_) => text().prop_map(Value::Text).boxed(),
        KeyType::Bytea => bytea().prop_map(Value::Bytea).boxed(),
        KeyType::Date => biased_i32().prop_map(Value::Date).boxed(),
        KeyType::Time => time_value().prop_map(Value::Time).boxed(),
        KeyType::Timestamp => biased_i64().prop_map(Value::Timestamp).boxed(),
        KeyType::TimestampTz => biased_i64().prop_map(Value::TimestampTz).boxed(),
        KeyType::Interval => interval().prop_map(Value::Interval).boxed(),
        KeyType::Uuid => uuid().prop_map(Value::Uuid).boxed(),
        KeyType::Jsonb => jsonb_value().prop_map(Value::Jsonb).boxed(),
        KeyType::Array(elem) => sql_array(*elem).prop_map(Value::Array).boxed(),
    }
}

pub fn optional_value(ty: KeyType) -> BoxedStrategy<Option<Value>> {
    prop_oneof![
        1 => Just(None),
        5 => typed_value(ty).prop_map(Some),
    ]
    .boxed()
}

/// Uniform choice among a non-empty set of strategies.
fn any_of(opts: Vec<BoxedStrategy<Value>>) -> BoxedStrategy<Value> {
    let n = opts.len() as u32;
    (0..n)
        .prop_flat_map(move |i| opts[i as usize].clone())
        .boxed()
}

/// An equal-value, different-representation variant of `a`, when the type
/// has representation freedom: float zero sign and NaN payload, numeric
/// display scale and negative zero, the interval month/day/time split,
/// jsonb input key order and duplicate keys, array element representations.
fn value_variant(ty: &KeyType, a: &Value) -> BoxedStrategy<Value> {
    match (ty, a) {
        (KeyType::Float8, Value::Float8(x)) => {
            if x.is_nan() {
                (
                    0x7FF0_0000_0000_0001u64..=0x7FFF_FFFF_FFFF_FFFF,
                    any::<bool>(),
                )
                    .prop_map(|(b, neg)| {
                        let sign = if neg { 0x8000_0000_0000_0000u64 } else { 0 };
                        Value::Float8(f64::from_bits(b | sign))
                    })
                    .boxed()
            } else if *x == 0.0 {
                Just(Value::Float8(-x)).boxed()
            } else {
                Just(a.clone()).boxed()
            }
        }
        (KeyType::Float4, Value::Float4(x)) => {
            if x.is_nan() {
                (0x7F80_0001u32..=0x7FFF_FFFF, any::<bool>())
                    .prop_map(|(b, neg)| {
                        let sign = if neg { 0x8000_0000u32 } else { 0 };
                        Value::Float4(f32::from_bits(b | sign))
                    })
                    .boxed()
            } else if *x == 0.0 {
                Just(Value::Float4(-x)).boxed()
            } else {
                Just(a.clone()).boxed()
            }
        }
        (KeyType::Numeric, Value::Numeric(Numeric::Finite(d))) => decimal_variants(d),
        (KeyType::Interval, Value::Interval(i)) => interval_variants(*i),
        (KeyType::Jsonb, Value::Jsonb(j)) => match jsonb_rep_variant(j) {
            Some(v) => prop_oneof![
                1 => Just(a.clone()),
                2 => Just(Value::Jsonb(v)),
            ]
            .boxed(),
            None => Just(a.clone()).boxed(),
        },
        (KeyType::Array(elem), Value::Array(arr)) => array_variant(elem, arr),
        _ => Just(a.clone()).boxed(),
    }
}

/// `1.0 <-> 1.00 <-> 01` (append/prepend a zero digit, shift the scale) and
/// `0 <-> -0`.
fn decimal_variants(d: &Decimal) -> BoxedStrategy<Value> {
    let mk = |dd: Decimal| Value::Numeric(Numeric::Finite(dd));
    let mut opts: Vec<BoxedStrategy<Value>> = vec![Just(mk(d.clone())).boxed()];
    let mut append = d.clone();
    if let Some(s) = append.scale.checked_add(1) {
        append.digits.push(0);
        append.scale = s;
        opts.push(Just(mk(append)).boxed());
    }
    let mut prepend = d.clone();
    prepend.digits.insert(0, 0);
    opts.push(Just(mk(prepend)).boxed());
    if is_zero(d) {
        let mut neg = d.clone();
        neg.negative = !neg.negative;
        opts.push(Just(mk(neg)).boxed());
    }
    any_of(opts)
}

/// Same span, different (months, days, time) split: `1 mon <-> 30 days`,
/// `1 day <-> 24 hours`.
fn interval_variants(i: Interval) -> BoxedStrategy<Value> {
    let day = Interval::USECS_PER_DAY;
    let mut opts: Vec<BoxedStrategy<Interval>> = Vec::new();
    if let (Some(m), Some(d)) = (i.months.checked_sub(1), i.days.checked_add(30)) {
        opts.push(
            Just(Interval {
                months: m,
                days: d,
                micros: i.micros,
            })
            .boxed(),
        );
    }
    if let (Some(m), Some(d)) = (i.months.checked_add(1), i.days.checked_sub(30)) {
        opts.push(
            Just(Interval {
                months: m,
                days: d,
                micros: i.micros,
            })
            .boxed(),
        );
    }
    if let (Some(d), Some(t)) = (i.days.checked_sub(1), i.micros.checked_add(day)) {
        opts.push(
            Just(Interval {
                months: i.months,
                days: d,
                micros: t,
            })
            .boxed(),
        );
    }
    if let (Some(d), Some(t)) = (i.days.checked_add(1), i.micros.checked_sub(day)) {
        opts.push(
            Just(Interval {
                months: i.months,
                days: d,
                micros: t,
            })
            .boxed(),
        );
    }
    if opts.is_empty() {
        Just(Value::Interval(i)).boxed()
    } else {
        any_of(
            opts.into_iter()
                .map(|s| s.prop_map(Value::Interval).boxed())
                .collect(),
        )
        .boxed()
    }
}

/// One representation change somewhere inside a jsonb value: a number's
/// display scale, an object's input key order (rotated when keys are
/// unique) or a duplicate of a key's last value, an array element's own
/// representation. Returns `None` when there is nothing to vary.
fn jsonb_rep_variant(j: &Jsonb) -> Option<Jsonb> {
    match j {
        Jsonb::Number(Numeric::Finite(d)) => {
            let mut dd = d.clone();
            dd.digits.push(0);
            dd.scale = d.scale.checked_add(1)?;
            if is_zero(d) {
                dd.negative = !d.negative;
            }
            Some(Jsonb::Number(Numeric::Finite(dd)))
        }
        Jsonb::Array(items) => {
            for (i, item) in items.iter().enumerate() {
                if let Some(v) = jsonb_rep_variant(item) {
                    let mut out = items.clone();
                    out[i] = v;
                    return Some(Jsonb::Array(out));
                }
            }
            None
        }
        Jsonb::Object(pairs) => {
            let unique = pairs.len() > 1 && {
                let mut keys: Vec<&str> = pairs.iter().map(|(k, _)| k.as_str()).collect();
                keys.sort_unstable();
                keys.windows(2).all(|w| w[0] != w[1])
            };
            if unique {
                let mut rotated = pairs.clone();
                rotated.rotate_left(1);
                return Some(Jsonb::Object(rotated));
            }
            // Append a duplicate of the first key's last value: last-wins
            // keeps the same canonical form.
            let (k0, _) = pairs.first()?;
            let last = pairs.iter().rev().find(|(k, _)| k == k0)?;
            let mut out = pairs.clone();
            out.push((k0.clone(), last.1.clone()));
            Some(Jsonb::Object(out))
        }
        _ => None,
    }
}

fn array_variant(elem: &KeyType, arr: &Array) -> BoxedStrategy<Value> {
    let idx = arr
        .elems
        .iter()
        .position(|e| matches!(e, Some(v) if admits_variant(elem, v)));
    match idx {
        None => Just(Value::Array(arr.clone())).boxed(),
        Some(i) => match &arr.elems[i] {
            Some(v) => {
                let base = arr.clone();
                value_variant(elem, v)
                    .prop_map(move |nv| {
                        let mut out = base.clone();
                        out.elems[i] = Some(nv);
                        Value::Array(out)
                    })
                    .boxed()
            }
            None => Just(Value::Array(arr.clone())).boxed(),
        },
    }
}

/// A value structurally close to `a` but generally not equal to it, so the
/// late tie-break rules get exercised: arrays with the same elements and
/// different dims (§7 rule 2), jsonb objects with the same values and
/// re-drawn keys (§6 rule 3 storage order). Other types: `a` itself.
fn near_value(ty: &KeyType, a: &Value) -> BoxedStrategy<Value> {
    match (ty, a) {
        (KeyType::Array(_), Value::Array(arr)) => array_redim(arr),
        (KeyType::Jsonb, Value::Jsonb(Jsonb::Object(pairs))) => {
            let values: Vec<Jsonb> = pairs.iter().map(|(_, v)| v.clone()).collect();
            vec(jsonb_key(), values.len()..=values.len())
                .prop_map(move |keys| {
                    Value::Jsonb(Jsonb::Object(
                        keys.into_iter().zip(values.iter().cloned()).collect(),
                    ))
                })
                .boxed()
        }
        _ => Just(a.clone()).boxed(),
    }
}

/// Same elements, different dimensions (not an equal value unless the dims
/// come out identical): exercises the §7 rule 2 tie-breaks on ndims, each
/// length and each lower bound.
fn array_redim(arr: &Array) -> BoxedStrategy<Value> {
    let n = arr.elems.len();
    let elems = arr.elems.clone();
    if n == 0 {
        return Just(Value::Array(arr.clone())).boxed();
    }
    let edge_lower = || {
        prop_oneof![
            Just(i32::MIN),
            Just(-1),
            Just(0),
            Just(1),
            Just(2),
            Just(i32::MAX - 26),
        ]
    };
    let mut shapes: Vec<BoxedStrategy<Vec<ArrayDim>>> = Vec::new();
    // Same lengths, new lower bounds.
    let lens: Vec<i32> = arr.dims.iter().map(|d| d.len).collect();
    shapes.push(
        vec(edge_lower(), lens.len()..=lens.len())
            .prop_map(move |lows| {
                lens.iter()
                    .zip(lows)
                    .map(|(&len, lower)| ArrayDim { len, lower })
                    .collect()
            })
            .boxed(),
    );
    // Same ndims, first length kept and the rest reversed (same product),
    // new lower bounds: a later length can differ while an earlier lower
    // bound differs too (all lengths compare before any lower bound).
    let rev: Vec<i32> = arr
        .dims
        .iter()
        .take(1)
        .chain(arr.dims.iter().skip(1).rev())
        .map(|d| d.len)
        .collect();
    shapes.push(
        vec(edge_lower(), rev.len()..=rev.len())
            .prop_map(move |lows| {
                rev.iter()
                    .zip(lows)
                    .map(|(&len, lower)| ArrayDim { len, lower })
                    .collect()
            })
            .boxed(),
    );
    // Flattened to one dimension, or one extra unit dimension.
    let n32 = n as i32;
    shapes.push(
        edge_lower()
            .prop_map(move |lower| vec![ArrayDim { len: n32, lower }])
            .boxed(),
    );
    if arr.dims.len() < 6 {
        let dims = arr.dims.clone();
        shapes.push(
            (any::<bool>(), edge_lower())
                .prop_map(move |(front, lower)| {
                    let mut d = dims.clone();
                    let unit = ArrayDim { len: 1, lower };
                    if front {
                        d.insert(0, unit);
                    } else {
                        d.push(unit);
                    }
                    d
                })
                .boxed(),
        );
    }
    let n_shapes = shapes.len();
    (0..n_shapes)
        .prop_flat_map(move |i| shapes[i].clone())
        .prop_map(move |dims| {
            Value::Array(Array {
                dims,
                elems: elems.clone(),
            })
        })
        .boxed()
}

fn admits_variant(elem: &KeyType, v: &Value) -> bool {
    match (elem, v) {
        (KeyType::Float8, Value::Float8(x)) => *x == 0.0 || x.is_nan(),
        (KeyType::Float4, Value::Float4(x)) => *x == 0.0 || x.is_nan(),
        (KeyType::Numeric, Value::Numeric(Numeric::Finite(_))) => true,
        (KeyType::Interval, Value::Interval(_)) => true,
        (KeyType::Jsonb, Value::Jsonb(j)) => jsonb_rep_variant(j).is_some(),
        _ => false,
    }
}

fn optional_variant(ty: &KeyType, a: &Option<Value>) -> BoxedStrategy<Option<Value>> {
    match a {
        None => Just(None).boxed(),
        Some(v) => prop_oneof![
            1 => Just(Some(v.clone())),
            2 => value_variant(ty, v).prop_map(Some),
            1 => near_value(ty, v).prop_map(Some),
        ]
        .boxed(),
    }
}

/// A pair of one type's values: independent values, identical values,
/// equal-value different-representation variants and near values.
pub fn typed_pair(ty: KeyType) -> BoxedStrategy<(Value, Value)> {
    typed_value(ty.clone())
        .prop_flat_map(move |a| {
            let b = prop_oneof![
                3 => typed_value(ty.clone()),
                2 => Just(a.clone()),
                3 => value_variant(&ty, &a),
                1 => near_value(&ty, &a),
            ];
            b.prop_map(move |b| (a.clone(), b))
        })
        .boxed()
}

pub fn optional_pair(ty: KeyType) -> BoxedStrategy<(Option<Value>, Option<Value>)> {
    optional_value(ty.clone())
        .prop_flat_map(move |a| {
            let b = prop_oneof![
                3 => optional_value(ty.clone()),
                2 => Just(a.clone()),
                3 => optional_variant(&ty, &a),
            ];
            b.prop_map(move |b| (a.clone(), b))
        })
        .boxed()
}

pub fn key_type_with_value() -> BoxedStrategy<(KeyType, Value)> {
    key_type()
        .prop_flat_map(|ty| typed_value(ty.clone()).prop_map(move |v| (ty.clone(), v)))
        .boxed()
}

pub fn key_type_with_pair() -> BoxedStrategy<(KeyType, Value, Value)> {
    key_type()
        .prop_flat_map(|ty| typed_pair(ty.clone()).prop_map(move |(a, b)| (ty.clone(), a, b)))
        .boxed()
}

pub fn key_type_with_optional_pair() -> BoxedStrategy<(KeyType, Option<Value>, Option<Value>)> {
    key_type()
        .prop_flat_map(|ty| optional_pair(ty.clone()).prop_map(move |(a, b)| (ty.clone(), a, b)))
        .boxed()
}

/// One key column with a random direction and NULLS placement.
pub fn key_column() -> BoxedStrategy<KeyColumn> {
    key_type()
        .prop_flat_map(|ty| {
            (any::<bool>(), any::<bool>()).prop_map(move |(desc, first)| KeyColumn {
                dir: if desc {
                    Direction::Desc
                } else {
                    Direction::Asc
                },
                nulls: if first { Nulls::First } else { Nulls::Last },
                ty: ty.clone(),
            })
        })
        .boxed()
}

pub fn column_with_optional_pair() -> BoxedStrategy<(KeyColumn, Option<Value>, Option<Value>)> {
    key_column()
        .prop_flat_map(|col| {
            optional_pair(col.ty.clone()).prop_map(move |(a, b)| (col.clone(), a, b))
        })
        .boxed()
}

/// Composite keys of 1-4 columns with random ASC/DESC and NULLS FIRST/LAST,
/// two value tuples for the same column list.
pub type KeyVals = Vec<Option<Value>>;

/// Each column's pair comes from `optional_pair`, so leading columns are
/// often equal (identical or variant) and later columns decide the order.
pub fn composite_key_pair() -> BoxedStrategy<(Vec<KeyColumn>, KeyVals, KeyVals)> {
    (1usize..=4)
        .prop_flat_map(|n| {
            vec(key_column(), n..=n).prop_flat_map(|cols| {
                let pairs: Vec<_> = cols.iter().map(|c| optional_pair(c.ty.clone())).collect();
                pairs.prop_map(move |pairs| {
                    let (va, vb): (KeyVals, KeyVals) = pairs.into_iter().unzip();
                    (cols.clone(), va, vb)
                })
            })
        })
        .boxed()
}
