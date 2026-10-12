//! Lossless round-trips through the row format (card C-T6a1): every
//! literal of every single-column golden file survives
//! `encode_row` -> `decode_row` (and `encode_value` -> `decode_value`)
//! with its representation intact — the point of C-T6 §2.1 against the
//! lossy key codec (C-Q3s §8.1): float `-0` and NaN payloads, numeric
//! display scale, the interval month/day/time split, jsonb input key
//! order and duplicate keys.

mod common;

use common::{parse_row, type_for};
use nucleus_codec::row::{
    decode_row, decode_value, encode_row, encode_value, value_repr_eq, ColumnDef, Datum,
};
use nucleus_codec::{KeyType, Numeric, Value};
use std::path::PathBuf;

fn col(ty: KeyType) -> ColumnDef {
    ColumnDef {
        attnum: 1,
        name: "c".to_string(),
        ty,
        not_null: false,
        dropped: false,
        missing: None,
    }
}

/// (stem, literal, value) of every golden row except `composite`.
fn golden_literals() -> Vec<(String, String, Option<Value>)> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/golden");
    let mut paths: Vec<_> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("read {dir:?}: {e}"))
        .map(|e| e.unwrap_or_else(|e| panic!("readdir: {e}")).path())
        .filter(|p| p.extension().is_some_and(|x| x == "tsv"))
        .collect();
    paths.sort();
    let mut out = Vec::new();
    for path in paths {
        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or_default()
            .to_string();
        if stem == "composite" {
            continue;
        }
        let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
        for line in text.lines() {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let lit = line.split('\t').nth(1).unwrap_or_else(|| panic!("{line}"));
            let vals = parse_row(&stem, lit);
            assert_eq!(1, vals.len(), "{stem} {lit}");
            out.push((
                stem.clone(),
                lit.to_string(),
                vals.into_iter().next().flatten(),
            ));
        }
    }
    out
}

fn assert_repr(stem: &str, lit: &str, got: &Option<Value>, want: &Option<Value>) {
    match (got, want) {
        (None, None) => {}
        (Some(g), Some(w)) => assert!(
            value_repr_eq(g, w),
            "{stem} {lit}: decoded {g:?} != original {w:?}"
        ),
        _ => panic!("{stem} {lit}: NULL-ness changed: {got:?} vs {want:?}"),
    }
}

#[test]
fn row_roundtrip() {
    let lits = golden_literals();
    assert!(
        lits.len() > 400,
        "expected the whole corpus, got {} literals",
        lits.len()
    );
    for (stem, lit, val) in &lits {
        let ty = type_for(stem);
        let c = col(ty.clone());
        let live: Vec<Option<Datum>> = vec![val.clone().map(Datum::Inline)];
        let bytes = encode_row(1, std::slice::from_ref(&c), &live)
            .unwrap_or_else(|e| panic!("{stem} {lit}: encode: {e}"));
        let back =
            decode_row(1, &[c], &bytes).unwrap_or_else(|e| panic!("{stem} {lit}: decode: {e}"));
        assert_eq!(1, back.len(), "{stem} {lit}");
        match (&back[0], val) {
            (None, None) => {}
            (Some(Datum::Inline(v)), w) => assert_repr(stem, lit, &Some(v.clone()), w),
            _ => panic!("{stem} {lit}: toast datum out of nowhere: {back:?}"),
        }
        // Value level, whole-slice.
        if let Some(w) = val {
            let mut vb = Vec::new();
            encode_value(&ty, w, &mut vb)
                .unwrap_or_else(|e| panic!("{stem} {lit}: encode_value: {e}"));
            let v = decode_value(&ty, &vb)
                .unwrap_or_else(|e| panic!("{stem} {lit}: decode_value: {e}"));
            assert_repr(stem, lit, &Some(v), &Some(w.clone()));
        }
    }
}

/// The representation freedoms C-Q3s §8.1 lists as lost by the key codec:
/// each must survive the row bytes, and the pairs named by the card must
/// stay distinct representations of equal SQL values.
#[test]
fn representation_freedoms() {
    // float: -0 vs 0, and NaN payloads (either sign, any quiet payload).
    for (a, b) in [
        (0x8000_0000_0000_0000u64, 0u64),                  // -0 vs 0
        (0x7FF8_0000_DEAD_BEEFu64, 0x7FF8_0000_0000_0000), // payload vs canonical
        (0xFFF8_0000_0000_0001u64, 0x7FF8_0000_0000_0000), // sign set
    ] {
        let (a, b) = (f64::from_bits(a), f64::from_bits(b));
        round("float8 pair", &KeyType::Float8, &Value::Float8(a));
        round("float8 pair", &KeyType::Float8, &Value::Float8(b));
        assert_repr_distinct(&Value::Float8(a), &Value::Float8(b));
    }
    for (a, b) in [
        (0x8000_0000u32, 0u32),
        (0xFFC0_0042u32, 0x7FC0_0000),
        (0x7F80_0001u32, 0x7FC0_0000),
    ] {
        let (a, b) = (f32::from_bits(a), f32::from_bits(b));
        round("float4 pair", &KeyType::Float4, &Value::Float4(a));
        round("float4 pair", &KeyType::Float4, &Value::Float4(b));
        assert_repr_distinct(&Value::Float4(a), &Value::Float4(b));
    }

    // numeric: display scale kept; 1.50, 1.5 and 1 are three rows.
    for lit in ["1.50", "1.5", "1", "0.00", "-0"] {
        let n = Numeric::parse(lit).unwrap_or_else(|| panic!("{lit}: bad numeric"));
        round("numeric", &KeyType::Numeric, &Value::Numeric(n));
    }

    // interval: '1 mon' and '30 days' are the same span, different triples;
    // each round-trips to itself.
    let mon = Value::Interval(nucleus_codec::Interval {
        months: 1,
        days: 0,
        micros: 0,
    });
    let days = Value::Interval(nucleus_codec::Interval {
        months: 0,
        days: 30,
        micros: 0,
    });
    round("interval", &KeyType::Interval, &mon);
    round("interval", &KeyType::Interval, &days);
    assert_repr_distinct(&mon, &days);

    // jsonb: input key order and duplicate keys survive (input semantics
    // are the key codec's business, C-Q3s §6 rule 7).
    use nucleus_codec::Jsonb;
    let dup = Value::Jsonb(Jsonb::Object(vec![
        ("b".to_string(), Jsonb::Bool(true)),
        ("a".to_string(), Jsonb::Null),
        ("b".to_string(), Jsonb::Bool(false)),
    ]));
    let sorted = Value::Jsonb(Jsonb::Object(vec![
        ("a".to_string(), Jsonb::Null),
        ("b".to_string(), Jsonb::Bool(false)),
    ]));
    round("jsonb", &KeyType::Jsonb, &dup);
    assert_repr_distinct(&dup, &sorted);

    // arrays: NULL elements, dimension lists and lower bounds.
    use nucleus_codec::{Array, ArrayDim};
    let a = Value::Array(Array {
        dims: vec![ArrayDim { len: 3, lower: 0 }],
        elems: vec![Some(Value::Int4(1)), None, Some(Value::Int4(-7))],
    });
    round("array", &KeyType::Array(Box::new(KeyType::Int4)), &a);
}

fn round(what: &str, ty: &KeyType, v: &Value) {
    let mut vb = Vec::new();
    encode_value(ty, v, &mut vb).unwrap_or_else(|e| panic!("{what}: encode_value: {e}"));
    let back = decode_value(ty, &vb).unwrap_or_else(|e| panic!("{what}: decode_value: {e}"));
    assert!(value_repr_eq(&back, v), "{what}: {back:?} != {v:?}");
    // And through a full row, which frames the same bytes in a datum.
    let c = col(ty.clone());
    let live = vec![Some(Datum::Inline(v.clone()))];
    let bytes = encode_row(1, std::slice::from_ref(&c), &live)
        .unwrap_or_else(|e| panic!("{what}: encode_row: {e}"));
    let back = decode_row(1, &[c], &bytes).unwrap_or_else(|e| panic!("{what}: decode_row: {e}"));
    match &back[0] {
        Some(Datum::Inline(got)) => {
            assert!(value_repr_eq(got, v), "{what}: row decode {got:?} != {v:?}")
        }
        _ => panic!("{what}: row decode lost the datum: {back:?}"),
    }
}

fn assert_repr_distinct(a: &Value, b: &Value) {
    // NB: Rust's `==` on Value would call -0 == 0 and NaN != NaN;
    // representation distinctness is exactly what value_repr_eq decides.
    assert!(
        !value_repr_eq(a, b),
        "{a:?} and {b:?} must stay distinct representations"
    );
}
