//! Error paths of the codec (card C-K1 work item 4), plus the §8.1 lossy
//! decoding table, the §8 hash, P-PREFIX spot checks and `decode_key_prefix`
//! with a trailing suffix (C-T0 §2.2 appends a ts after the key).

use nucleus_codec::{
    decode_key, decode_key_prefix, encode_key, hash_value, Array, ArrayDim, Collation, Decimal,
    Interval, Jsonb, KeyColumn, KeyType, Nulls, Numeric, Value,
};

fn asc(ty: KeyType) -> Vec<KeyColumn> {
    vec![KeyColumn::asc(ty)]
}

fn dec1(ty: KeyType, bytes: &[u8]) -> Result<Vec<Option<Value>>, nucleus_codec::CodecError> {
    decode_key(&asc(ty), bytes)
}

fn dec_err(ty: KeyType, bytes: &[u8]) -> nucleus_codec::CodecError {
    match dec1(ty, bytes) {
        Ok(v) => panic!("expected error for {bytes:02x?}, got {v:?}"),
        Err(e) => e,
    }
}

fn enc1(ty: KeyType, v: Value) -> Vec<u8> {
    let mut out = Vec::new();
    encode_key(&asc(ty.clone()), &[Some(v)], &mut out)
        .unwrap_or_else(|e| panic!("encode {ty:?}: {e}"));
    out
}

fn enc_err(cols: &[KeyColumn], vals: &[Option<Value>]) -> nucleus_codec::CodecError {
    let mut out = Vec::new();
    match encode_key(cols, vals, &mut out) {
        Ok(()) => panic!("expected error, encoded {out:02x?}"),
        Err(e) => e,
    }
}

fn dec_num(s: &str) -> Decimal {
    match Numeric::parse(s) {
        Some(Numeric::Finite(d)) => d,
        _ => panic!("{s}: not finite"),
    }
}

// ---- Arity ----

#[test]
fn arity_mismatch() {
    let cols = vec![KeyColumn::asc(KeyType::Int4), KeyColumn::asc(KeyType::Int8)];
    assert_eq!(
        nucleus_codec::CodecError::Arity {
            expected: 2,
            got: 1
        },
        enc_err(&cols, &[Some(Value::Int4(1))])
    );
    assert_eq!(
        nucleus_codec::CodecError::Arity {
            expected: 2,
            got: 3
        },
        enc_err(
            &cols,
            &[
                Some(Value::Int4(1)),
                Some(Value::Int8(2)),
                Some(Value::Int4(3))
            ]
        )
    );
}

// ---- TypeMismatch (encode) ----

#[test]
fn type_mismatch_on_encode() {
    for (ty, v) in [
        (KeyType::Bool, Value::Int4(1)),
        (KeyType::Int2, Value::Bool(true)),
        (KeyType::Int4, Value::Int8(1)),
        (KeyType::Int8, Value::Int4(1)),
        (KeyType::Float4, Value::Float8(1.0)),
        (KeyType::Float8, Value::Int8(1)),
        (KeyType::Numeric, Value::Float8(1.0)),
        (KeyType::Text(Collation::C), Value::Bytea(vec![1])),
        (KeyType::Bytea, Value::Text("a".into())),
        (KeyType::Date, Value::Int4(0)),
        (KeyType::Time, Value::Timestamp(0)),
        (KeyType::Timestamp, Value::Time(0)),
        (KeyType::TimestampTz, Value::Timestamp(0)),
        (KeyType::Interval, Value::Int8(0)),
        (KeyType::Uuid, Value::Bytea(vec![0; 16])),
        (KeyType::Jsonb, Value::Text("1".into())),
        (KeyType::Array(Box::new(KeyType::Int4)), Value::Int4(1)),
        (
            KeyType::Array(Box::new(KeyType::Text(Collation::C))),
            Value::Text("x".into()),
        ),
        // Array elements must match the element type.
        (
            KeyType::Array(Box::new(KeyType::Int4)),
            Value::Array(Array::from_elems(vec![Some(Value::Text("x".into()))])),
        ),
    ] {
        assert_eq!(
            nucleus_codec::CodecError::TypeMismatch,
            enc_err(&asc(ty.clone()), &[Some(v)]),
            "{ty:?}"
        );
    }
}

// ---- InvalidValue (encode) ----

#[test]
fn invalid_time_domain() {
    for v in [-1i64, 86_400_000_001, i64::MIN, i64::MAX] {
        let e = enc_err(&asc(KeyType::Time), &[Some(Value::Time(v))]);
        assert!(
            matches!(e, nucleus_codec::CodecError::InvalidValue(_)),
            "{e:?}"
        );
    }
}

#[test]
fn invalid_numeric_values() {
    for d in [
        Decimal {
            negative: false,
            digits: vec![10],
            scale: 0,
        },
        // digits.len() - scale overflows i32
        Decimal {
            negative: false,
            digits: vec![1],
            scale: i32::MIN,
        },
    ] {
        let e = enc_err(
            &asc(KeyType::Numeric),
            &[Some(Value::Numeric(Numeric::Finite(d)))],
        );
        assert!(
            matches!(e, nucleus_codec::CodecError::InvalidValue(_)),
            "{e:?}"
        );
    }
}

#[test]
fn invalid_jsonb_numbers_and_depth() {
    for n in [Numeric::NaN, Numeric::PosInf, Numeric::NegInf] {
        let e = enc_err(
            &asc(KeyType::Jsonb),
            &[Some(Value::Jsonb(Jsonb::Number(n)))],
        );
        assert!(
            matches!(e, nucleus_codec::CodecError::InvalidValue(_)),
            "{e:?}"
        );
    }
    // MAX_JSONB_DEPTH is 1000 (encode.rs); 1001 nested arrays exceed it.
    let mut j = Jsonb::Null;
    for _ in 0..=1000 {
        j = Jsonb::Array(vec![j]);
    }
    let e = enc_err(&asc(KeyType::Jsonb), &[Some(Value::Jsonb(j))]);
    assert!(
        matches!(e, nucleus_codec::CodecError::InvalidValue(_)),
        "{e:?}"
    );
}

#[test]
fn invalid_arrays() {
    let arr = |elems: Vec<Option<i32>>, dims: &[(i32, i32)]| {
        Some(Value::Array(Array {
            dims: dims
                .iter()
                .map(|&(len, lower)| ArrayDim { len, lower })
                .collect(),
            elems: elems.into_iter().map(|e| e.map(Value::Int4)).collect(),
        }))
    };
    let int4 = KeyType::Array(Box::new(KeyType::Int4));
    for (name, val) in [
        ("too many dims", arr(vec![Some(1)], &[(1, 1); 7])),
        ("dims on empty", arr(vec![], &[(0, 1)])),
        ("dims on empty 2", arr(vec![], &[(1, 1)])),
        ("no dims", arr(vec![Some(1)], &[])),
        ("product", arr(vec![Some(1)], &[(2, 1)])),
        (
            "product 2x2",
            arr(vec![Some(1), Some(2), Some(3)], &[(2, 1), (2, 1)]),
        ),
        ("len 0", arr(vec![Some(1), Some(2)], &[(0, 1)])),
        ("upper", arr(vec![Some(1)], &[(2, i32::MAX)])),
    ] {
        let e = enc_err(&asc(int4.clone()), &[val]);
        assert!(
            matches!(e, nucleus_codec::CodecError::InvalidValue(_)),
            "{name}: {e:?}"
        );
    }
    // arrays of arrays are not a type
    let e = enc_err(
        &asc(KeyType::Array(Box::new(KeyType::Array(Box::new(
            KeyType::Int4,
        ))))),
        &[Some(Value::Array(Array::from_elems(vec![Some(
            Value::Int4(1),
        )])))],
    );
    assert!(
        matches!(e, nucleus_codec::CodecError::InvalidValue(_)),
        "{e:?}"
    );
}

// ---- Truncated (decode) ----

#[test]
fn truncated_keys() {
    for (ty, full) in [
        (KeyType::Bool, enc1(KeyType::Bool, Value::Bool(true))),
        (KeyType::Int2, enc1(KeyType::Int2, Value::Int2(1))),
        (KeyType::Int4, enc1(KeyType::Int4, Value::Int4(1))),
        (KeyType::Int8, enc1(KeyType::Int8, Value::Int8(1))),
        (KeyType::Date, enc1(KeyType::Date, Value::Date(1))),
        (KeyType::Time, enc1(KeyType::Time, Value::Time(1))),
        (
            KeyType::Timestamp,
            enc1(KeyType::Timestamp, Value::Timestamp(1)),
        ),
        (
            KeyType::TimestampTz,
            enc1(KeyType::TimestampTz, Value::TimestampTz(1)),
        ),
        (KeyType::Float4, enc1(KeyType::Float4, Value::Float4(1.0))),
        (KeyType::Float8, enc1(KeyType::Float8, Value::Float8(1.0))),
        (KeyType::Uuid, enc1(KeyType::Uuid, Value::Uuid([1; 16]))),
        (
            KeyType::Interval,
            enc1(
                KeyType::Interval,
                Value::Interval(Interval {
                    months: 1,
                    days: 2,
                    micros: 3,
                }),
            ),
        ),
        (
            KeyType::Numeric,
            enc1(
                KeyType::Numeric,
                Value::Numeric(Numeric::Finite(dec_num("1.5"))),
            ),
        ),
        (
            KeyType::Text(Collation::C),
            enc1(KeyType::Text(Collation::C), Value::Text("ab".into())),
        ),
        (
            KeyType::Bytea,
            enc1(KeyType::Bytea, Value::Bytea(vec![0, 1, 2])),
        ),
        (
            KeyType::Jsonb,
            enc1(
                KeyType::Jsonb,
                Value::Jsonb(Jsonb::Array(vec![Jsonb::Null])),
            ),
        ),
        (
            KeyType::Array(Box::new(KeyType::Int4)),
            enc1(
                KeyType::Array(Box::new(KeyType::Int4)),
                Value::Array(Array::from_elems(vec![Some(Value::Int4(1))])),
            ),
        ),
    ] {
        assert_eq!(
            nucleus_codec::CodecError::Truncated,
            dec_err(ty.clone(), &full[..full.len() - 1]),
            "{ty:?}"
        );
    }
    // marker alone, empty input, escaped cut after 0x00, numeric cut in the
    // mantissa, array cut in the dims
    assert_eq!(
        nucleus_codec::CodecError::Truncated,
        dec_err(KeyType::Int4, &[0x01])
    );
    assert_eq!(
        nucleus_codec::CodecError::Truncated,
        dec_err(KeyType::Int4, &[])
    );
    assert_eq!(
        nucleus_codec::CodecError::Truncated,
        dec_err(KeyType::Text(Collation::C), &[0x01, b'a', 0x00])
    );
    assert_eq!(
        nucleus_codec::CodecError::Truncated,
        dec_err(
            KeyType::Numeric,
            &[0x01, 0x04, 0x80, 0x00, 0x00, 0x01, 0x0b]
        )
    );
    assert_eq!(
        nucleus_codec::CodecError::Truncated,
        dec_err(
            KeyType::Array(Box::new(KeyType::Int4)),
            &[0x01, 0x01, 0x80, 0x00, 0x00, 0x01, 0x00, 0x01]
        )
    );
}

// ---- Trailing / prefix ----

#[test]
fn trailing_bytes_and_prefix_decode() {
    let mut key = enc1(KeyType::Int4, Value::Int4(-7));
    key.push(0xAA);
    assert_eq!(
        nucleus_codec::CodecError::Trailing(1),
        dec_err(KeyType::Int4, &key)
    );
    // C-T0 §2.2: a ts suffix after the prefix; decode_key_prefix accepts it.
    let cols = vec![
        KeyColumn::asc(KeyType::Text(Collation::C)),
        KeyColumn::asc(KeyType::Int4),
    ];
    let vals = [Some(Value::Text("k".into())), Some(Value::Int4(5))];
    let mut buf = Vec::new();
    encode_key(&cols, &vals, &mut buf).unwrap_or_else(|e| panic!("{e}"));
    buf.extend_from_slice(&42u64.to_be_bytes());
    let (out, used) = decode_key_prefix(&cols, &buf).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(vals.to_vec(), out);
    assert_eq!(buf.len() - 8, used);
    assert_eq!(
        nucleus_codec::CodecError::Trailing(8),
        match decode_key(&cols, &buf) {
            Err(e) => e,
            Ok(v) => panic!("expected error, got {v:?}"),
        }
    );
}

// ---- Malformed (decode) ----

fn dec_err_cols(cols: &[KeyColumn], bytes: &[u8]) -> nucleus_codec::CodecError {
    match decode_key(cols, bytes) {
        Ok(v) => panic!("expected error for {bytes:02x?}, got {v:?}"),
        Err(e) => e,
    }
}

#[test]
fn malformed_markers_and_scalars() {
    // column markers: 0x03 never valid; 0x00 only for NULLS FIRST columns;
    // 0x02 only for NULLS LAST columns.
    assert_eq!(
        nucleus_codec::CodecError::Malformed("column marker"),
        dec_err_cols(&[KeyColumn::asc(KeyType::Int4)], &[0x03, 0, 0, 0, 0])
    );
    assert_eq!(
        nucleus_codec::CodecError::Malformed("column marker"),
        dec_err_cols(&[KeyColumn::asc(KeyType::Int4)], &[0x00, 0, 0, 0, 0])
    );
    assert_eq!(
        nucleus_codec::CodecError::Malformed("column marker"),
        dec_err_cols(
            &[KeyColumn::asc(KeyType::Int4).with_nulls(Nulls::First)],
            &[0x02, 0, 0, 0, 0]
        )
    );
    // the matching NULL markers decode to NULL in both placements
    assert_eq!(
        vec![None],
        decode_key(&[KeyColumn::asc(KeyType::Int4)], &[0x02]).unwrap_or_else(|e| panic!("{e}"))
    );
    assert_eq!(
        vec![None],
        decode_key(
            &[KeyColumn::asc(KeyType::Int4).with_nulls(Nulls::First)],
            &[0x00]
        )
        .unwrap_or_else(|e| panic!("{e}"))
    );
    // bool
    assert_eq!(
        nucleus_codec::CodecError::Malformed("bool"),
        dec_err(KeyType::Bool, &[0x01, 2])
    );
    // escape
    assert_eq!(
        nucleus_codec::CodecError::Malformed("escape"),
        dec_err(KeyType::Bytea, &[0x01, b'a', 0x00, 0x02])
    );
    // text must be UTF-8
    assert_eq!(
        nucleus_codec::CodecError::Malformed("text is not UTF-8"),
        dec_err(KeyType::Text(Collation::C), &[0x01, 0xFF, 0x00, 0x01])
    );
    // time domain holds on decode too (§4 row)
    assert_eq!(
        nucleus_codec::CodecError::Malformed("time out of range"),
        dec_err(
            KeyType::Time,
            &[0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]
        )
    );
    // no finite interval has this span: the canonical representative does
    // not fit the (month, day, time) fields.
    assert_eq!(
        nucleus_codec::CodecError::Malformed("interval out of range"),
        dec_err(
            KeyType::Interval,
            &[
                0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                0xFF, 0xFF, 0xFF
            ]
        )
    );
}

#[test]
fn malformed_floats_non_canonical() {
    // -0 must encode as +0; NaN must be the canonical quiet NaN.
    let neg_zero_asc = {
        let mut k = vec![0x01];
        k.extend_from_slice(&(!0x8000_0000_0000_0000u64).to_be_bytes());
        k
    };
    assert_eq!(
        nucleus_codec::CodecError::Malformed("non-canonical float8"),
        dec_err(KeyType::Float8, &neg_zero_asc)
    );
    let nan_payload_asc = {
        let mut k = vec![0x01];
        k.extend_from_slice(&(0x7FF8_0000_0000_0001u64 ^ (1u64 << 63)).to_be_bytes());
        k
    };
    assert_eq!(
        nucleus_codec::CodecError::Malformed("non-canonical float8"),
        dec_err(KeyType::Float8, &nan_payload_asc)
    );
    let neg_zero_f32 = {
        let mut k = vec![0x01];
        k.extend_from_slice(&(!0x8000_0000u32).to_be_bytes());
        k
    };
    assert_eq!(
        nucleus_codec::CodecError::Malformed("non-canonical float4"),
        dec_err(KeyType::Float4, &neg_zero_f32)
    );
    // the same two patterns inside a DESC column (bytes inverted by the
    // column direction) are caught through the mask.
    let desc_col = [KeyColumn::desc(KeyType::Float8)];
    let stored: Vec<u8> = std::iter::once(0x01)
        .chain(neg_zero_asc[1..].iter().map(|b| !b))
        .collect();
    assert!(matches!(
        decode_key(&desc_col, &stored),
        Err(nucleus_codec::CodecError::Malformed(_))
    ));
}

#[test]
fn malformed_numerics() {
    let num = |bytes: &[u8]| {
        let mut k = vec![0x01];
        k.extend_from_slice(bytes);
        k
    };
    // unknown tag
    assert_eq!(
        nucleus_codec::CodecError::Malformed("numeric tag"),
        dec_err(KeyType::Numeric, &num(&[0x07]))
    );
    // digit pair byte out of 1..=100
    assert_eq!(
        nucleus_codec::CodecError::Malformed("numeric digit pair"),
        dec_err(KeyType::Numeric, &num(&[0x04, 0x80, 0, 0, 0, 1, 105, 0]))
    );
    // mantissa may not start with a zero digit (0.0x is not normalised)
    assert_eq!(
        nucleus_codec::CodecError::Malformed("non-canonical numeric"),
        dec_err(
            KeyType::Numeric,
            &num(&[0x04, 0x80, 0, 0, 0, 1, 0x02, 0x0b, 0])
        )
    );
    // mantissa may not end with a zero digit (0.10 -> 0.1)
    assert_eq!(
        nucleus_codec::CodecError::Malformed("non-canonical numeric"),
        dec_err(
            KeyType::Numeric,
            &num(&[0x04, 0x80, 0, 0, 0, 1, 0x0b, 0x01, 0])
        )
    );
    // empty mantissa
    assert_eq!(
        nucleus_codec::CodecError::Malformed("non-canonical numeric"),
        dec_err(KeyType::Numeric, &num(&[0x04, 0x80, 0, 0, 0, 1, 0]))
    );
}

/// A tiny jsonb literal hook (the full parser lives in the golden tests).
fn jsonb_lit(lit: &str) -> Jsonb {
    let t = lit.trim();
    if t == "null" {
        return Jsonb::Null;
    }
    if t == "true" || t == "false" {
        return Jsonb::Bool(t == "true");
    }
    if let Some(inner) = t.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
        return Jsonb::String(inner.to_string());
    }
    if let Some(inner) = t.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
        let mut items = Vec::new();
        if !inner.is_empty() {
            for part in inner.split(',') {
                items.push(jsonb_lit(part));
            }
        }
        return Jsonb::Array(items);
    }
    if let Some(inner) = t.strip_prefix('{').and_then(|s| s.strip_suffix('}')) {
        let mut pairs = Vec::new();
        if !inner.is_empty() {
            for part in inner.split(',') {
                let (k, v) = part
                    .split_once(':')
                    .unwrap_or_else(|| panic!("{lit}: pair"));
                pairs.push((k.trim().trim_matches('"').to_string(), jsonb_lit(v)));
            }
        }
        return Jsonb::Object(pairs);
    }
    match Numeric::parse(t) {
        Some(n) => Jsonb::Number(n),
        None => panic!("{lit}: bad jsonb literal"),
    }
}

#[test]
fn malformed_jsonb() {
    let j = |lit: &str| enc1(KeyType::Jsonb, Value::Jsonb(jsonb_lit(lit)));
    // unknown value tag
    let mut bad = j("null");
    bad[2] = 0x00;
    assert_eq!(
        nucleus_codec::CodecError::Malformed("jsonb tag"),
        dec_err(KeyType::Jsonb, &bad)
    );
    // top-level class mismatch: scalar bytes claiming container class
    let mut bad = j("1");
    bad[1] = 0x03;
    assert_eq!(
        nucleus_codec::CodecError::Malformed("jsonb top-level class"),
        dec_err(KeyType::Jsonb, &bad)
    );
    // bool payload
    let mut bad = j("true");
    bad[3] = 2;
    assert_eq!(
        nucleus_codec::CodecError::Malformed("jsonb bool"),
        dec_err(KeyType::Jsonb, &bad)
    );
    // keys out of storage order: {"a":1,"b":2} with the key bytes swapped
    let mut bad = j("{\"a\":1,\"b\":2}");
    let pos_a = bad
        .iter()
        .position(|&b| b == b'a')
        .unwrap_or_else(|| panic!("a"));
    let pos_b = bad
        .iter()
        .position(|&b| b == b'b')
        .unwrap_or_else(|| panic!("b"));
    bad[pos_a] = b'b';
    bad[pos_b] = b'a';
    assert_eq!(
        nucleus_codec::CodecError::Malformed("jsonb keys not in storage order"),
        dec_err(KeyType::Jsonb, &bad)
    );
    // duplicate keys
    let mut bad = j("{\"a\":1,\"b\":2}");
    let pos_b = bad
        .iter()
        .position(|&b| b == b'b')
        .unwrap_or_else(|| panic!("b"));
    bad[pos_b] = b'a';
    assert_eq!(
        nucleus_codec::CodecError::Malformed("jsonb keys not in storage order"),
        dec_err(KeyType::Jsonb, &bad)
    );
}

// ---- Malformed arrays (decode) ----

#[test]
fn malformed_arrays() {
    let int4 = KeyType::Array(Box::new(KeyType::Int4));
    // one valid element then a bad marker
    assert_eq!(
        nucleus_codec::CodecError::Malformed("array element marker"),
        dec_err(int4.clone(), &[0x01, 0x01, 0x80, 0x00, 0x00, 0x01, 0x03])
    );
    // ndims 7
    assert!(matches!(
        dec_err(int4.clone(), &[0x01, 0x00, 0x07]),
        nucleus_codec::CodecError::Malformed(_)
    ));
    // element count does not match dims (len 2 declared, 1 element)
    assert!(matches!(
        dec_err(
            int4,
            &[
                0x01, 0x01, 0x80, 0x00, 0x00, 0x01, // {1}
                0x00, 0x01, 0x80, 0x00, 0x00, 0x02, // end, 1 dim, len 2
                0x80, 0x00, 0x00, 0x01, // lower 1
            ]
        ),
        nucleus_codec::CodecError::Malformed(_)
    ));
}

// ---- §8.1 lossy decoding returns the canonical form ----

#[test]
fn lossy_decode_canonical_forms() {
    // numeric: display scale is lost, '1.50' decodes as '1.5'
    let n = enc1(
        KeyType::Numeric,
        Value::Numeric(Numeric::Finite(dec_num("1.50"))),
    );
    match dec1(KeyType::Numeric, &n) {
        Ok(v) => match v[0].as_ref() {
            Some(Value::Numeric(Numeric::Finite(d))) => {
                assert_eq!("1.5", format!("{d}"));
                assert!(!d.negative);
                assert_eq!(vec![1, 5], d.digits);
                assert_eq!(1, d.scale);
            }
            other => panic!("{other:?}"),
        },
        Err(e) => panic!("{e}"),
    }
    // negative zero: one encoding, one value
    let z = enc1(
        KeyType::Numeric,
        Value::Numeric(Numeric::Finite(dec_num("-0.00"))),
    );
    assert_eq!(vec![0x01, 0x03], z);
    // float: -0 -> +0, NaN -> canonical quiet NaN
    let f = enc1(KeyType::Float8, Value::Float8(-0.0));
    assert_eq!(vec![0x01, 0x80, 0, 0, 0, 0, 0, 0, 0], f);
    match dec1(KeyType::Float8, &f) {
        Ok(v) => assert_eq!(Some(&Value::Float8(0.0)), v[0].as_ref()),
        Err(e) => panic!("{e}"),
    }
    let nan = enc1(
        KeyType::Float8,
        Value::Float8(f64::from_bits(0x7FF8_0000_0000_0042)),
    );
    assert_eq!(
        vec![0x01, 0xFF, 0xF8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
        nan
    );
    // interval: the month/day/time split is lost, the span is kept
    let iv = |months, days, micros| {
        Value::Interval(Interval {
            months,
            days,
            micros,
        })
    };
    for (input, expect) in [
        (iv(0, 0, 36 * 3_600_000_000), iv(0, 1, 12 * 3_600_000_000)),
        (iv(0, 0, 90 * 60_000_000), iv(0, 0, 90 * 60_000_000)),
        (iv(1, 0, 0), iv(1, 0, 0)),
        (iv(0, 0, -1), iv(0, -1, 86_400_000_000 - 1)),
    ] {
        let enc = enc1(KeyType::Interval, input.clone());
        match dec1(KeyType::Interval, &enc) {
            Ok(v) => assert_eq!(Some(&expect), v[0].as_ref(), "{input:?}"),
            Err(e) => panic!("{e}"),
        }
    }
    // jsonb: keys come back in storage order, duplicates removed
    let j = enc1(
        KeyType::Jsonb,
        Value::Jsonb(Jsonb::Object(vec![
            ("b".into(), Jsonb::Bool(true)),
            ("a".into(), Jsonb::Null),
            ("b".into(), Jsonb::Bool(false)),
        ])),
    );
    match dec1(KeyType::Jsonb, &j) {
        Ok(v) => match v[0].as_ref() {
            Some(Value::Jsonb(Jsonb::Object(pairs))) => {
                assert_eq!(2, pairs.len());
                assert_eq!("a", pairs[0].0);
                assert_eq!(Jsonb::Null, pairs[0].1);
                assert_eq!("b", pairs[1].0);
                assert_eq!(Jsonb::Bool(false), pairs[1].1);
            }
            other => panic!("{other:?}"),
        },
        Err(e) => panic!("{e}"),
    }
}

// ---- §8 hash ----

#[test]
fn hash_follows_sql_equality() {
    let h = |v: &str| {
        hash_value(
            &KeyType::Numeric,
            &Value::Numeric(Numeric::parse(v).unwrap_or_else(|| panic!("{v}"))),
        )
        .unwrap_or_else(|e| panic!("{e}"))
    };
    assert_eq!(h("1"), h("1.0"));
    assert_eq!(h("1"), h("1.00"));
    assert_eq!(h("1"), h("01"));
    assert_eq!(h("0"), h("-0.000"));
    assert_ne!(h("1"), h("2"));

    let hf = |a: f64, b: f64| {
        hash_value(&KeyType::Float8, &Value::Float8(a)).unwrap_or_else(|e| panic!("{e}"))
            == hash_value(&KeyType::Float8, &Value::Float8(b)).unwrap_or_else(|e| panic!("{e}"))
    };
    assert!(hf(-0.0, 0.0));
    assert!(hf(f64::from_bits(0x7FF8_0000_0000_0001), f64::NAN));
    assert!(!hf(1.0, 2.0));

    let ht = |a: &str, b: &str| {
        hash_value(&KeyType::Text(Collation::C), &Value::Text(a.into()))
            .unwrap_or_else(|e| panic!("{e}"))
            == hash_value(&KeyType::Text(Collation::C), &Value::Text(b.into()))
                .unwrap_or_else(|e| panic!("{e}"))
    };
    assert!(ht("a", "a"));
    assert!(!ht("a", "ab"));
}

// ---- P-PREFIX ----

#[test]
fn encodings_are_prefix_free() {
    let pairs: Vec<(KeyType, Value, Value)> = vec![
        (KeyType::Bool, Value::Bool(false), Value::Bool(true)),
        (KeyType::Int4, Value::Int4(0), Value::Int4(1)),
        (KeyType::Int8, Value::Int8(-1), Value::Int8(1)),
        (KeyType::Float8, Value::Float8(0.0), Value::Float8(1.0)),
        (
            KeyType::Numeric,
            Value::Numeric(Numeric::Finite(dec_num("1"))),
            Value::Numeric(Numeric::Finite(dec_num("1.0001"))),
        ),
        (
            KeyType::Text(Collation::C),
            Value::Text("a".into()),
            Value::Text("ab".into()),
        ),
        (
            KeyType::Bytea,
            Value::Bytea(vec![0]),
            Value::Bytea(vec![0, 0]),
        ),
        (
            KeyType::Uuid,
            Value::Uuid([0; 16]),
            Value::Uuid([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]),
        ),
        (
            KeyType::Jsonb,
            Value::Jsonb(Jsonb::Array(vec![Jsonb::Number(Numeric::Finite(dec_num(
                "1",
            )))])),
            Value::Jsonb(Jsonb::Array(vec![
                Jsonb::Number(Numeric::Finite(dec_num("1"))),
                Jsonb::Number(Numeric::Finite(dec_num("0"))),
            ])),
        ),
        (
            KeyType::Array(Box::new(KeyType::Int4)),
            Value::Array(Array::from_elems(vec![Some(Value::Int4(1))])),
            Value::Array(Array::from_elems(vec![Some(Value::Int4(1)), None])),
        ),
    ];
    for (ty, a, b) in pairs {
        let (ea, eb) = (enc1(ty.clone(), a), enc1(ty.clone(), b));
        assert!(
            !ea.starts_with(&eb) && !eb.starts_with(&ea),
            "{ty:?}: {ea:02x?} vs {eb:02x?}"
        );
    }
}

// ---- DESC inversion and NULLS markers (§2) ----

#[test]
fn desc_inverts_value_bytes_only() {
    let v = Value::Text("ab".into());
    let a = enc1(KeyType::Text(Collation::C), v.clone());
    let d = {
        let mut out = Vec::new();
        encode_key(
            &[KeyColumn::desc(KeyType::Text(Collation::C))],
            &[Some(v)],
            &mut out,
        )
        .unwrap_or_else(|e| panic!("{e}"));
        out
    };
    assert_eq!(a[0], d[0]); // marker untouched
    assert_eq!(
        a[1..].to_vec(),
        d[1..].iter().map(|b| !b).collect::<Vec<u8>>()
    );
    // decode a DESC key back
    match decode_key(&[KeyColumn::desc(KeyType::Text(Collation::C))], &d) {
        Ok(vals) => assert_eq!(Some(Value::Text("ab".into())), vals[0]),
        Err(e) => panic!("{e}"),
    }
    // mixed directions in one composite key: each column inverted on its own
    let cols = vec![
        KeyColumn::asc(KeyType::Int4),
        KeyColumn::desc(KeyType::Int4),
        KeyColumn::asc(KeyType::Int4),
    ];
    let vals = [
        Some(Value::Int4(1)),
        Some(Value::Int4(2)),
        Some(Value::Int4(3)),
    ];
    let mut mixed = Vec::new();
    encode_key(&cols, &vals, &mut mixed).unwrap_or_else(|e| panic!("{e}"));
    let asc_cols = vec![
        KeyColumn::asc(KeyType::Int4),
        KeyColumn::asc(KeyType::Int4),
        KeyColumn::asc(KeyType::Int4),
    ];
    let mut all_asc = Vec::new();
    encode_key(&asc_cols, &vals, &mut all_asc).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(mixed[..5].to_vec(), all_asc[..5].to_vec()); // col 1
    assert_eq!(mixed[5], all_asc[5]); // marker never inverted
    assert_eq!(
        mixed[6..10].to_vec(),
        all_asc[6..10].iter().map(|b| !b).collect::<Vec<u8>>()
    ); // col 2 value bytes inverted
    assert_eq!(mixed[10..].to_vec(), all_asc[10..].to_vec()); // col 3
                                                              // direction affects only non-NULL columns; NULL uses the placement marker
    let mut nf = Vec::new();
    encode_key(
        &[KeyColumn::desc(KeyType::Int4).with_nulls(Nulls::Last)],
        &[None],
        &mut nf,
    )
    .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(vec![0x02], nf);
}
