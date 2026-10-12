//! Row-format property tests (card C-T6a1): random column lists and
//! values round-trip through `encode_row` / `decode_row` by
//! representation (C-T6 §2.1 vs the lossy key codec, C-Q3s §8.1), and
//! `decode_row_raw` agrees with the typed decode. Strategies are the
//! reference corpus-biased ones (C-K2), which include the representation
//! freedoms: float zeros and NaN payloads, scale variants, interval
//! splits, jsonb duplicate keys, NULL elements and dimension edges.
//!
//! 10_000 cases per property by default; `NUCLEUS_CODEC_CASES` overrides.

mod reference;

use nucleus_codec::row::{
    decode_row, decode_row_raw, decode_value, encode_row, value_repr_eq, ColumnDef, Datum, RawDatum,
};
use nucleus_codec::{KeyType, Value};
use proptest::collection::vec;
use proptest::prelude::*;
use reference::{key_type, optional_value, typed_value};

fn cfg() -> ProptestConfig {
    let cases = std::env::var("NUCLEUS_CODEC_CASES")
        .ok()
        .map(|s| {
            s.parse::<u32>()
                .unwrap_or_else(|e| panic!("bad NUCLEUS_CODEC_CASES {s:?}: {e}"))
        })
        .unwrap_or(10_000);
    let mut config = ProptestConfig::with_cases(cases);
    // The golden corpus pins the bytes; failing seeds would add files
    // without adding information.
    config.failure_persistence = None;
    config
}

/// One column of the encoding definition: a type, the value written (a
/// NOT NULL column always gets one), and the flags.
#[derive(Debug)]
struct Spec {
    ty: KeyType,
    val: Option<Value>,
    not_null: bool,
    dropped: bool,
}

fn spec() -> BoxedStrategy<Spec> {
    key_type()
        .prop_flat_map(|ty| {
            (
                optional_value(ty.clone()),
                typed_value(ty.clone()),
                any::<bool>(),
                any::<bool>(),
            )
                .prop_map(move |(opt, fallback, not_null, dropped)| {
                    // §3: DROP COLUMN clears not_null; a NOT NULL column is
                    // never written NULL.
                    let not_null = not_null && !dropped;
                    Spec {
                        val: if not_null { Some(fallback) } else { opt },
                        ty: ty.clone(),
                        not_null,
                        dropped,
                    }
                })
        })
        .boxed()
}

/// A column added after the row was written: type plus optional missing
/// default.
type ExtraCol = (KeyType, Option<Value>);

fn extra() -> BoxedStrategy<ExtraCol> {
    key_type()
        .prop_flat_map(|ty| optional_value(ty.clone()).prop_map(move |v| (ty.clone(), v)))
        .boxed()
}

/// (version, the columns the row was written under, columns added since).
type RowCase = (u32, Vec<Spec>, Vec<ExtraCol>);

fn row_case() -> BoxedStrategy<RowCase> {
    (1u32..=1000, vec(spec(), 0..=6), vec(extra(), 0..=2)).boxed()
}

fn def_of(specs: &[Spec], extras: &[ExtraCol]) -> Vec<ColumnDef> {
    let mut cols: Vec<ColumnDef> = specs
        .iter()
        .enumerate()
        .map(|(i, s)| ColumnDef {
            attnum: i as u16 + 1,
            name: format!("c{}", i + 1),
            ty: s.ty.clone(),
            not_null: s.not_null,
            dropped: s.dropped,
            missing: None,
        })
        .collect();
    for (i, (ty, missing)) in extras.iter().enumerate() {
        cols.push(ColumnDef {
            attnum: (specs.len() + i + 1) as u16,
            name: format!("c{}", specs.len() + i + 1),
            ty: ty.clone(),
            not_null: false,
            dropped: false,
            missing: missing.clone(),
        });
    }
    cols
}

fn live_of(specs: &[Spec]) -> Vec<Option<Datum>> {
    specs
        .iter()
        .filter(|s| !s.dropped)
        .map(|s| s.val.clone().map(Datum::Inline))
        .collect()
}

proptest! {
    #![proptest_config(cfg())]

    /// Random column lists and values round-trip by representation, and
    /// the encoding is deterministic. Decoding under a definition with
    /// extra columns serves `missing` (else NULL) for attnums past the
    /// row's `natts` (§3 decode rule).
    #[test]
    fn rows_round_trip((version, specs, extras) in row_case()) {
        // Written under the original definition, read under one that has
        // since grown (§3 decode rule: attnums past natts get `missing`).
        let write_def = def_of(&specs, &[]);
        let read_def = def_of(&specs, &extras);
        let live = live_of(&specs);
        let bytes = encode_row(version, &write_def, &live)
            .unwrap_or_else(|e| panic!("encode: {e:?}"));
        let again = encode_row(version, &write_def, &live)
            .unwrap_or_else(|e| panic!("encode: {e:?}"));
        prop_assert!(bytes == again);

        let back = decode_row(version, &read_def, &bytes)
            .unwrap_or_else(|e| panic!("decode: {e:?}"));
        let want: Vec<Option<Datum>> = specs
            .iter()
            .filter(|s| !s.dropped)
            .map(|s| s.val.clone().map(Datum::Inline))
            .chain(
                // Columns beyond natts: the missing default, else NULL.
                extras.iter().map(|(_, m)| m.clone().map(Datum::Inline)),
            )
            .collect();
        prop_assert_eq!(back.len(), want.len());
        for (i, (got, w)) in back.iter().zip(&want).enumerate() {
            match (got, w) {
                (None, None) => {}
                (Some(Datum::Inline(g)), Some(Datum::Inline(w))) => {
                    prop_assert!(value_repr_eq(g, w), "column {}: {g:?} != {w:?}", i + 1);
                }
                _ => prop_assert!(false, "column {}: {got:?} vs {w:?}", i + 1),
            }
        }
    }

    /// `decode_row_raw` returns every attnum `<= natts` (dropped ones are
    /// NULL by the write rule) and its inline bytes decode under the
    /// column's type to the original value.
    #[test]
    fn raw_rows_match((version, specs, _extras) in row_case()) {
        let cols = def_of(&specs, &[]);
        let live = live_of(&specs);
        let bytes = encode_row(version, &cols, &live)
            .unwrap_or_else(|e| panic!("encode: {e:?}"));
        let (header, raw) = decode_row_raw(&bytes)
            .unwrap_or_else(|e| panic!("raw decode: {e:?}"));
        prop_assert_eq!(header.schema_version, version);
        prop_assert_eq!(usize::from(header.natts), specs.len());
        prop_assert_eq!(raw.len(), specs.len());
        for (i, spec) in specs.iter().enumerate() {
            let entry = &raw[i];
            if spec.dropped {
                prop_assert!(entry.is_none(), "dropped attnum {} written non-NULL", i + 1);
                continue;
            }
            match (&spec.val, entry) {
                (None, None) => {}
                (Some(w), Some(RawDatum::Inline(value_bytes))) => {
                    let g = decode_value(&spec.ty, value_bytes)
                        .unwrap_or_else(|e| panic!("attnum {}: {e:?}", i + 1));
                    prop_assert!(value_repr_eq(&g, w), "attnum {}: {g:?} != {w:?}", i + 1);
                }
                _ => prop_assert!(false, "attnum {}: {entry:?} vs {:?}", i + 1, spec.val),
            }
        }
    }
}
