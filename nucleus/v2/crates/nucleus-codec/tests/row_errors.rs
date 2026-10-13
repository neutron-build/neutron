//! Error paths of the row format (card C-T6a1): truncation at every
//! offset, trailing bytes, unknown datum tags, NOT NULL violations, the
//! §3 decode rule's error cases, toast pointers, `decode_row_raw` and
//! `missing`, and the §2.1 value-level failures. Everything returns
//! `Err`, nothing panics (C-T6 §2: XX001, never a panic).

use nucleus_codec::row::{
    decode_row, decode_row_raw, decode_value, encode_row, encode_value, ColumnDef, Datum, RawDatum,
    RowError, ToastPointer, ValueError,
};
use nucleus_codec::{Collation, Decimal, KeyType, Numeric, Value};

fn col(attnum: u16, ty: KeyType) -> ColumnDef {
    ColumnDef {
        attnum,
        name: format!("c{attnum}"),
        ty,
        not_null: false,
        dropped: false,
        missing: None,
    }
}

fn cols_def(tys: &[KeyType]) -> Vec<ColumnDef> {
    tys.iter()
        .enumerate()
        .map(|(i, ty)| col(i as u16 + 1, ty.clone()))
        .collect()
}

fn inline(vals: Vec<Option<Value>>) -> Vec<Option<Datum>> {
    vals.into_iter().map(|v| v.map(Datum::Inline)).collect()
}

fn num(s: &str) -> Numeric {
    Numeric::parse(s).unwrap_or_else(|| panic!("{s}: bad numeric"))
}

fn enc(cols: &[ColumnDef], live: &[Option<Datum>]) -> Vec<u8> {
    encode_row(1, cols, live).unwrap_or_else(|e| panic!("encode: {e:?}"))
}

fn dec(cols: &[ColumnDef], bytes: &[u8]) -> Result<Vec<Option<Datum>>, RowError> {
    decode_row(1, cols, bytes)
}

/// A handful of rows of different shapes, all valid.
fn sample_rows() -> Vec<(Vec<ColumnDef>, Vec<u8>)> {
    let mut out = Vec::new();
    let mut add = |cols: Vec<ColumnDef>, live: Vec<Option<Datum>>| {
        let bytes = enc(&cols, &live);
        out.push((cols, bytes));
    };
    // 3 live columns, all present.
    add(
        cols_def(&[KeyType::Int4, KeyType::Text(Collation::C), KeyType::Float8]),
        inline(vec![
            Some(Value::Int4(-1)),
            Some(Value::Text("row value\0".to_string())),
            Some(Value::Float8(f64::MIN)),
        ]),
    );
    // 5 columns: NULLs, a dropped column and an array.
    let mut cols = cols_def(&[
        KeyType::Bool,
        KeyType::Numeric,
        KeyType::Array(Box::new(KeyType::Int4)),
        KeyType::Uuid,
        KeyType::Interval,
    ]);
    cols[2].dropped = true;
    add(
        cols,
        inline(vec![
            None,
            Some(Value::Numeric(num("1.50"))),
            None,
            Some(Value::Interval(nucleus_codec::Interval {
                months: 1,
                days: -2,
                micros: 3,
            })),
        ]),
    );
    // A jsonb and a bytea column.
    add(
        cols_def(&[KeyType::Jsonb, KeyType::Bytea]),
        inline(vec![
            Some(Value::Jsonb(nucleus_codec::Jsonb::Array(vec![
                nucleus_codec::Jsonb::Null,
                nucleus_codec::Jsonb::Object(vec![(
                    "k".to_string(),
                    nucleus_codec::Jsonb::Bool(true),
                )]),
            ]))),
            Some(Value::Bytea(vec![0, 0xFF, 0])),
        ]),
    );
    // All NULL.
    add(
        cols_def(&[KeyType::Int2, KeyType::Date, KeyType::Time]),
        inline(vec![None, None, None]),
    );
    // A toast datum among inline ones.
    add(
        cols_def(&[KeyType::Text(Collation::C), KeyType::Int8]),
        vec![
            Some(Datum::Toast(ToastPointer {
                id: [9u8; 20],
                raw_len: 100_000,
                chunks: 13,
            })),
            Some(Datum::Inline(Value::Int8(-9))),
        ],
    );
    // An empty definition: header only, natts 0.
    add(Vec::new(), Vec::new());
    out
}

#[test]
fn samples_decode_cleanly() {
    for (cols, bytes) in sample_rows() {
        dec(&cols, &bytes).unwrap_or_else(|e| panic!("sample decode: {e:?}"));
        decode_row_raw(&bytes).unwrap_or_else(|e| panic!("sample raw decode: {e:?}"));
    }
}

/// Truncation at every offset of every sample row is an error for
/// `decode_row` and `decode_row_raw`; below the 8-byte header it is one
/// for `row_header` too. Never a panic.
#[test]
fn truncation_at_every_offset() {
    use nucleus_codec::row::row_header;
    for (cols, bytes) in sample_rows() {
        for cut in 0..bytes.len() {
            let cut_bytes = &bytes[..cut];
            assert!(
                dec(&cols, cut_bytes).is_err(),
                "decode_row accepted cut {cut} of {bytes:02x?}"
            );
            assert!(
                decode_row_raw(cut_bytes).is_err(),
                "decode_row_raw accepted cut {cut} of {bytes:02x?}"
            );
            if cut < 8 {
                assert!(
                    row_header(cut_bytes).is_err(),
                    "row_header accepted cut {cut} of {bytes:02x?}"
                );
            }
        }
    }
}

#[test]
fn trailing_bytes_rejected() {
    for (cols, bytes) in sample_rows() {
        for extra in [vec![0x00], vec![0xFF], vec![0x52, 0x01], vec![0x00; 12]] {
            let mut b = bytes.clone();
            b.extend_from_slice(&extra);
            assert!(
                dec(&cols, &b).is_err(),
                "decode_row accepted trailing {extra:02x?} after {bytes:02x?}"
            );
            assert!(
                decode_row_raw(&b).is_err(),
                "decode_row_raw accepted trailing {extra:02x?}"
            );
        }
    }
}

#[test]
fn unknown_datum_tag() {
    // A one-column int4 row; flip its datum tag.
    let cols = cols_def(&[KeyType::Int4]);
    let mut bytes = enc(&cols, &inline(vec![Some(Value::Int4(5))]));
    let datum_tag = 8 /* header */ + 1 /* nullmap */;
    for bad in [0x02u8, 0x7F, 0xFF] {
        bytes[datum_tag] = bad;
        assert_eq!(
            Err(RowError::Corrupt("unknown datum tag")),
            dec(&cols, &bytes),
            "tag {bad:#x}"
        );
        assert_eq!(
            Err(RowError::Corrupt("unknown datum tag")),
            decode_row_raw(&bytes),
            "tag {bad:#x}"
        );
    }
    // A toast tag with a short pointer.
    bytes[datum_tag] = 0x01;
    bytes.truncate(bytes.len() - 4);
    assert_eq!(
        Err(RowError::Corrupt("truncated toast pointer")),
        decode_row_raw(&bytes)
    );
    // An inline length that overruns the row.
    bytes[datum_tag] = 0x00;
    bytes[datum_tag + 1] = 0xFF;
    assert_eq!(
        Err(RowError::Corrupt("truncated datum")),
        decode_row_raw(&bytes)
    );
}

#[test]
fn not_null_violations() {
    // Encode side: a NULL for a live NOT NULL column is refused.
    let mut cols = cols_def(&[KeyType::Int4, KeyType::Text(Collation::C)]);
    cols[1].not_null = true;
    assert_eq!(
        Err(RowError::NotNull { attnum: 2 }),
        encode_row(1, &cols, &inline(vec![Some(Value::Int4(1)), None]))
    );
    // Decode side: a NULL bit on a live NOT NULL column is corrupt.
    let nullable = cols_def(&[KeyType::Int4, KeyType::Text(Collation::C)]);
    let bytes = enc(&nullable, &inline(vec![Some(Value::Int4(1)), None]));
    assert_eq!(
        Err(RowError::Corrupt("NULL bit on a NOT NULL column")),
        dec(&cols, &bytes)
    );
    // Dropped columns are written NULL without complaint (§3 clears
    // not_null on drop; the writer does not check dropped attnums), and
    // decode skips them.
    let mut dropped = cols_def(&[KeyType::Int4, KeyType::Text(Collation::C)]);
    dropped[1].dropped = true;
    dropped[1].not_null = true;
    let bytes = enc(&dropped, &inline(vec![Some(Value::Int4(1))]));
    assert_eq!(
        Ok(vec![Some(Datum::Inline(Value::Int4(1)))]),
        dec(&dropped, &bytes)
    );
}

#[test]
fn toast_pointer_round_trip() {
    let cols = cols_def(&[KeyType::Text(Collation::C), KeyType::Int8]);
    let ptr = ToastPointer {
        id: [
            0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0x63, 0, 0, 0, 4, 0, 0, 0, 0, 9,
        ],
        raw_len: 8192 * 2 + 12,
        chunks: 3,
    };
    let live: Vec<Option<Datum>> = vec![
        Some(Datum::Toast(ptr)),
        Some(Datum::Inline(Value::Int8(i64::MIN))),
    ];
    let bytes = encode_row(4, &cols, &live).unwrap_or_else(|e| panic!("encode: {e:?}"));
    assert_eq!(Ok(live), decode_row(4, &cols, &bytes));
    let (header, raw) = decode_row_raw(&bytes).unwrap_or_else(|e| panic!("raw: {e:?}"));
    assert_eq!((4, 2), (header.schema_version, header.natts));
    assert_eq!(
        vec![
            Some(RawDatum::Toast(ptr)),
            Some(RawDatum::Inline(i64::MIN.to_be_bytes().to_vec()))
        ],
        raw
    );
    // Truncating anywhere inside the 28-byte pointer is an error.
    let end = bytes.len();
    let ptr_at = 8 + 1 + 13; // header, nullmap, the int8 datum (tag+len+value)
    for cut in ptr_at + 1..end {
        assert!(decode_row_raw(&bytes[..cut]).is_err(), "cut {cut}");
    }
}

#[test]
fn decode_row_raw_returns_dropped_attnums() {
    // A row written before the drop: attnum 2 still holds a datum.
    let before = cols_def(&[KeyType::Int4, KeyType::Float4]);
    let bytes = enc(
        &before,
        &inline(vec![Some(Value::Int4(3)), Some(Value::Float4(0.5))]),
    );
    let (header, raw) = decode_row_raw(&bytes).unwrap_or_else(|e| panic!("raw: {e:?}"));
    assert_eq!(2, header.natts);
    assert_eq!(
        vec![
            Some(RawDatum::Inline(3i32.to_be_bytes().to_vec())),
            Some(RawDatum::Inline(0.5f32.to_bits().to_be_bytes().to_vec()))
        ],
        raw
    );

    // Under a definition that has since dropped attnum 2, decode_row
    // returns one entry and stays in sync; the raw row still has both.
    let mut after = cols_def(&[KeyType::Int4, KeyType::Float4]);
    after[1].dropped = true;
    assert_eq!(
        Ok(vec![Some(Datum::Inline(Value::Int4(3)))]),
        dec(&after, &bytes)
    );
    assert_eq!(
        2,
        decode_row_raw(&bytes)
            .unwrap_or_else(|e| panic!("{e:?}"))
            .1
            .len()
    );

    // A row written after the drop: attnum 2 is NULL, no datum.
    let bytes2 = enc(&after, &inline(vec![Some(Value::Int4(4))]));
    let (_, raw2) = decode_row_raw(&bytes2).unwrap_or_else(|e| panic!("{e:?}"));
    assert_eq!(
        vec![Some(RawDatum::Inline(4i32.to_be_bytes().to_vec())), None],
        raw2
    );

    // A row whose nullmap claims a datum for a dropped attnum that has
    // none: an error, not a silent misparse (and never a panic).
    let mut no_datum = bytes2.clone();
    no_datum[8] = 0x00; // nullmap: no NULLs
    assert!(dec(&after, &no_datum).is_err());
    assert!(decode_row_raw(&no_datum).is_err());
}

#[test]
fn missing_served_for_attnum_beyond_natts() {
    // Row written under a 1-column definition.
    let old = cols_def(&[KeyType::Int4]);
    let bytes = enc(&old, &inline(vec![Some(Value::Int4(6))]));

    // Current definition has a 2nd column with a missing default.
    let mut grown = cols_def(&[KeyType::Int4, KeyType::Text(Collation::C)]);
    grown[1].missing = Some(Value::Text("added later".to_string()));
    assert_eq!(
        Ok(vec![
            Some(Datum::Inline(Value::Int4(6))),
            Some(Datum::Inline(Value::Text("added later".to_string())))
        ]),
        dec(&grown, &bytes)
    );

    // Without a default the column reads as NULL.
    assert_eq!(
        Ok(vec![Some(Datum::Inline(Value::Int4(6))), None]),
        dec(
            &cols_def(&[KeyType::Int4, KeyType::Text(Collation::C)]),
            &bytes
        )
    );
}

#[test]
fn version_and_arity() {
    let cols = cols_def(&[KeyType::Int4]);
    let bytes = encode_row(2, &cols, &inline(vec![Some(Value::Int4(1))]))
        .unwrap_or_else(|e| panic!("encode: {e:?}"));
    // Row ahead of the definition.
    assert_eq!(
        Err(RowError::VersionAhead { row: 2, schema: 1 }),
        dec(&cols, &bytes)
    );
    // Definition at or past the row's version (lazy upgrade: v <= V).
    assert!(decode_row(2, &cols, &bytes).is_ok());
    assert!(decode_row(u32::MAX, &cols, &bytes).is_ok());
    // Row with more attnums than the definition.
    let wide = cols_def(&[KeyType::Int4, KeyType::Int4]);
    let wide_bytes = enc(&wide, &inline(vec![Some(Value::Int4(1)), None]));
    assert_eq!(
        Err(RowError::Corrupt("row natts exceeds the definition")),
        dec(&cols, &wide_bytes)
    );
    // Arity: one entry per live column.
    assert_eq!(
        Err(RowError::Arity {
            expected: 1,
            got: 0
        }),
        encode_row(1, &cols, &[])
    );
    assert_eq!(
        Err(RowError::Arity {
            expected: 1,
            got: 2
        }),
        encode_row(
            1,
            &cols,
            &inline(vec![Some(Value::Int4(1)), Some(Value::Int4(2))])
        )
    );
}

#[test]
fn row_header_basics() {
    use nucleus_codec::row::{row_header, RowHeader};
    let cols = cols_def(&[KeyType::Int4, KeyType::Int4]);
    let bytes = encode_row(
        0x0BAD_C0DE,
        &cols,
        &inline(vec![Some(Value::Int4(1)), Some(Value::Int4(2))]),
    )
    .unwrap_or_else(|e| panic!("encode: {e:?}"));
    assert_eq!(
        Ok(RowHeader {
            schema_version: 0x0BAD_C0DE,
            natts: 2
        }),
        row_header(&bytes)
    );
    let mut bad_magic = bytes.clone();
    bad_magic[0] = 0x53;
    assert_eq!(Err(RowError::Corrupt("row magic")), row_header(&bad_magic));
    assert_eq!(
        Err(RowError::Corrupt("truncated row header")),
        row_header(&bytes[..7])
    );
}

#[test]
fn type_mismatch_on_encode() {
    let cols = cols_def(&[KeyType::Text(Collation::C), KeyType::Int4]);
    assert_eq!(
        Err(RowError::TypeMismatch { attnum: 1 }),
        encode_row(
            1,
            &cols,
            &inline(vec![Some(Value::Int4(1)), Some(Value::Int4(2))])
        )
    );
    assert_eq!(
        Err(RowError::TypeMismatch { attnum: 2 }),
        encode_row(
            1,
            &cols,
            &inline(vec![
                Some(Value::Text("x".to_string())),
                Some(Value::Bool(true))
            ])
        )
    );
}

#[test]
fn invalid_definitions() {
    let mut cols = cols_def(&[KeyType::Int4, KeyType::Int4]);
    cols[1].attnum = 3;
    assert_eq!(
        Err(RowError::InvalidDdl("attnums must be 1..=n in order")),
        encode_row(1, &cols, &inline(vec![Some(Value::Int4(1))]))
    );
    let ok = cols_def(&[KeyType::Int4]);
    let bytes = enc(&ok, &inline(vec![Some(Value::Int4(1))]));
    assert_eq!(
        Err(RowError::InvalidDdl("attnums must be 1..=n in order")),
        dec(&cols, &bytes)
    );
}

/// F2 (C-T6a1 gate M-2): a pre-drop row with a toast pointer at an
/// attnum that is later dropped, followed by a live inline column.
/// `decode_row` returns the live value and skips the pointer;
/// `decode_row_raw` keeps it (the §5 old-pointer sweep); truncation
/// inside the skipped pointer is an error, never a misparse.
#[test]
fn dropped_toasted_column() {
    let before = cols_def(&[KeyType::Text(Collation::C), KeyType::Int8]);
    let ptr = ToastPointer {
        id: [7u8; 20],
        raw_len: 100_000,
        chunks: 13,
    };
    let live: Vec<Option<Datum>> = vec![
        Some(Datum::Toast(ptr)),
        Some(Datum::Inline(Value::Int8(-9))),
    ];
    let bytes = encode_row(3, &before, &live).unwrap_or_else(|e| panic!("encode: {e:?}"));

    let mut after = cols_def(&[KeyType::Text(Collation::C), KeyType::Int8]);
    after[0].dropped = true;
    assert_eq!(
        Ok(vec![Some(Datum::Inline(Value::Int8(-9)))]),
        decode_row(3, &after, &bytes),
        "decode_row must return the live value past the dropped pointer"
    );
    let (_, raw) = decode_row_raw(&bytes).unwrap_or_else(|e| panic!("raw: {e:?}"));
    assert_eq!(
        vec![
            Some(RawDatum::Toast(ptr)),
            Some(RawDatum::Inline((-9i64).to_be_bytes().to_vec()))
        ],
        raw
    );

    // Truncation anywhere inside the skipped pointer is an error for
    // the typed decode too (it must stay in sync with the raw one).
    let ptr_at = 8 + 1; // header, nullmap
    for cut in ptr_at + 1..bytes.len() {
        assert!(
            decode_row(3, &after, &bytes[..cut]).is_err(),
            "decode_row accepted cut {cut}"
        );
    }
}

/// F3 (C-T6a1 gate M-3): array shape is validated on both sides of the
/// row value bytes — the key codec's `check_dims` rules (C-Q3s §7):
/// at most 6 dimensions, lengths >= 1, no dimensions iff empty, and the
/// dims product must equal the element count.
#[test]
fn array_shape_rejected() {
    use nucleus_codec::{Array, ArrayDim};
    let arr = KeyType::Array(Box::new(KeyType::Int4));

    // One 1-dim int4 array value bytes: ndims, (len, lower), nelems,
    // nullmap, length-prefixed elements.
    fn array_bytes(dims: &[(i32, i32)], nelems: usize, nullmap: u8, elems: &[i32]) -> Vec<u8> {
        let mut b = vec![dims.len() as u8];
        for &(len, lower) in dims {
            b.extend_from_slice(&len.to_be_bytes());
            b.extend_from_slice(&lower.to_be_bytes());
        }
        b.extend_from_slice(&(nelems as u32).to_be_bytes());
        b.push(nullmap);
        for &e in elems {
            b.extend_from_slice(&4u32.to_be_bytes());
            b.extend_from_slice(&e.to_be_bytes());
        }
        b
    }

    // Decode side.
    let seven_dims = [(1i32, 1i32); 7];
    assert_eq!(
        Err(ValueError::Corrupt("too many array dimensions")),
        decode_value(&arr, &array_bytes(&seven_dims, 7, 0, &[7; 1]))
    );
    assert_eq!(
        Err(ValueError::Corrupt("array dimension out of range")),
        decode_value(&arr, &array_bytes(&[(-1, 1)], 1, 0, &[1]))
    );
    assert_eq!(
        Err(ValueError::Corrupt(
            "array dimensions do not match element count"
        )),
        decode_value(&arr, &array_bytes(&[(5, 1)], 1, 0, &[1]))
    );
    assert_eq!(
        Err(ValueError::Corrupt("empty array must have no dimensions")),
        decode_value(&arr, &array_bytes(&[], 2, 0, &[1, 2]))
    );
    // The same errors through a full row (the datum frames the bytes).
    let cols = cols_def(&[KeyType::Array(Box::new(KeyType::Int4))]);
    let mut row = Vec::new();
    row.extend_from_slice(&[0x52, 0x01, 0, 0, 0, 1, 0, 1, 0]);
    row.push(0x00); // tag inline
    let vb = array_bytes(&[(5, 1)], 1, 0, &[1]);
    row.extend_from_slice(&(vb.len() as u32).to_be_bytes());
    row.extend_from_slice(&vb);
    assert_eq!(
        Err(RowError::Corrupt(
            "array dimensions do not match element count"
        )),
        dec(&cols, &row)
    );

    // Encode side: shape is refused before any byte is written.
    let shaped = |dims: Vec<ArrayDim>, elems: Vec<Option<Value>>| {
        encode_value(&arr, &Value::Array(Array { dims, elems }), &mut Vec::new())
    };
    // The positive control first: a well-shaped array still encodes
    // (and decodes).
    assert_eq!(
        Ok(()),
        shaped(
            vec![ArrayDim { len: 2, lower: 1 }],
            vec![Some(Value::Int4(1)), None]
        )
    );
    assert!(decode_value(&arr, &array_bytes(&[(2, 1)], 2, 0b10, &[1])).is_ok());
    // 7 dimensions.
    assert_eq!(
        Err(ValueError::Corrupt("too many array dimensions")),
        shaped(
            vec![ArrayDim { len: 1, lower: 1 }; 7],
            vec![Some(Value::Int4(1))]
        )
    );
    // A negative dimension length.
    assert_eq!(
        Err(ValueError::Corrupt("array dimension out of range")),
        shaped(
            vec![ArrayDim { len: -1, lower: 1 }],
            vec![Some(Value::Int4(1))]
        )
    );
    // The dims product must equal the element count.
    assert_eq!(
        Err(ValueError::Corrupt(
            "array dimensions do not match element count"
        )),
        shaped(
            vec![ArrayDim { len: 5, lower: 1 }],
            vec![Some(Value::Int4(1))]
        )
    );
    // Dimensions without elements.
    assert_eq!(
        Err(ValueError::Corrupt("empty array must have no dimensions")),
        shaped(vec![ArrayDim { len: 1, lower: 1 }], vec![])
    );
    // And through a full row.
    let mut cols = cols_def(&[KeyType::Array(Box::new(KeyType::Int4))]);
    cols[0].not_null = false;
    assert_eq!(
        Err(RowError::Corrupt(
            "array dimensions do not match element count"
        )),
        encode_row(
            1,
            &cols,
            &[Some(Datum::Inline(Value::Array(Array {
                dims: vec![ArrayDim { len: 5, lower: 1 }],
                elems: vec![Some(Value::Int4(1))],
            })))],
        )
    );
}

/// F4 (C-T6a1 gate L-1): the jsonb depth cap — 1000 nesting levels
/// decode (and encode) fine, 1001 and 1,000,000 are errors, and
/// nothing aborts.
#[test]
fn jsonb_depth_cap() {
    use nucleus_codec::Jsonb;
    // k nested one-element arrays around a null scalar: the scalar sits
    // at depth k, the cap rejects depth > 1000.
    fn nested_jsonb(k: usize) -> Jsonb {
        let mut j = Jsonb::Null;
        for _ in 0..k {
            j = Jsonb::Array(vec![j]);
        }
        j
    }
    fn nested_bytes(k: usize) -> Vec<u8> {
        let mut b = Vec::with_capacity(5 * k + 1);
        for _ in 0..k {
            b.push(0x05); // array tag
            b.extend_from_slice(&1u32.to_be_bytes());
        }
        b.push(0x00); // null
        b
    }

    let deep = Value::Jsonb(nested_jsonb(1000));
    let mut vb = Vec::new();
    encode_value(&KeyType::Jsonb, &deep, &mut vb)
        .unwrap_or_else(|e| panic!("1000 levels must encode: {e:?}"));
    assert_eq!(
        nested_bytes(1000),
        vb,
        "the bytes are k array headers + null"
    );
    assert!(decode_value(&KeyType::Jsonb, &vb).is_ok());

    let too_deep = Value::Jsonb(nested_jsonb(1001));
    assert_eq!(
        Err(ValueError::Corrupt("jsonb nesting too deep")),
        encode_value(&KeyType::Jsonb, &too_deep, &mut Vec::new())
    );
    assert_eq!(
        Err(ValueError::Corrupt("jsonb nesting too deep")),
        decode_value(&KeyType::Jsonb, &nested_bytes(1001))
    );
    // A crafted million-level value errors without unwinding the stack.
    assert_eq!(
        Err(ValueError::Corrupt("jsonb nesting too deep")),
        decode_value(&KeyType::Jsonb, &nested_bytes(1_000_000))
    );
}

/// F4 (C-T6a1 gate L-3): §2's "bits past natts are 0" is enforced for
/// the row nullmap and the array nullmap alike.
#[test]
fn nullmap_padding_rejected() {
    use nucleus_codec::{Array, ArrayDim};

    // Row: a 1-column NULL row (nullmap 01); bit 1 is padding.
    let cols = cols_def(&[KeyType::Int4]);
    let bytes = enc(&cols, &inline(vec![None]));
    let mut bad = bytes.clone();
    bad[8] = 0x03;
    assert_eq!(
        Err(RowError::Corrupt("nullmap padding bits set")),
        dec(&cols, &bad)
    );
    assert_eq!(
        Err(RowError::Corrupt("nullmap padding bits set")),
        decode_row_raw(&bad).map(|_| ())
    );

    // Row: 9 columns, all NULL. Valid padding (byte 1 = 0x01, bit 8)
    // decodes; bit 9 (0x02) is padding and rejected.
    let nine: Vec<KeyType> = (0..9).map(|_| KeyType::Int4).collect();
    let cols9 = cols_def(&nine);
    let wide = enc(&cols9, &vec![None; 9]);
    assert_eq!(&[0xFF, 0x01], &wide[8..10], "the valid nullmap");
    assert!(dec(&cols9, &wide).is_ok());
    let mut bad_wide = wide.clone();
    bad_wide[9] = 0x03;
    assert_eq!(
        Err(RowError::Corrupt("nullmap padding bits set")),
        dec(&cols9, &bad_wide)
    );

    // Array: {1, NULL} (nelems 2, nullmap 0b10); bit 2 is padding.
    let arr = KeyType::Array(Box::new(KeyType::Int4));
    let good = Value::Array(Array {
        dims: vec![ArrayDim { len: 2, lower: 1 }],
        elems: vec![Some(Value::Int4(1)), None],
    });
    let mut vb = Vec::new();
    encode_value(&arr, &good, &mut vb).unwrap_or_else(|e| panic!("{e:?}"));
    // ndims(1) + dim(8) + nelems(4) + nullmap(1) + one element (4 + 4).
    assert_eq!(Some(&0b10u8), vb.get(vb.len() - 9), "the nullmap byte");
    assert!(decode_value(&arr, &vb).is_ok());
    let mut bad_arr = vb.clone();
    let nullmap_at = bad_arr.len() - 9;
    bad_arr[nullmap_at] = 0b110;
    assert_eq!(
        Err(ValueError::Corrupt("nullmap padding bits set")),
        decode_value(&arr, &bad_arr)
    );
}

/// §2.1 value-level failures: structurally broken byte sequences are
/// errors, including truncation at every offset for the framed types.
#[test]
fn value_bytes_errors() {
    let err = |ty: &KeyType, bytes: &[u8]| decode_value(ty, bytes).is_err();
    assert!(err(&KeyType::Bool, &[0x02]));
    assert!(err(&KeyType::Bool, &[]));
    assert!(err(&KeyType::Int4, &[0; 3]));
    assert!(err(&KeyType::Int4, &[0; 5]));
    assert!(err(&KeyType::Uuid, &[0; 15]));
    assert!(err(&KeyType::Interval, &[0; 15]));
    assert!(err(&KeyType::Float8, &[0; 7]));
    assert!(err(&KeyType::Text(Collation::C), &[0xFF, 0xFE, 0xFD]));
    assert!(err(&KeyType::Numeric, &[0x04])); // unknown tag
    assert!(err(&KeyType::Numeric, &[0x03, 0x02])); // bad sign byte
    assert!(err(
        &KeyType::Numeric,
        &[0x03, 0x00, 0, 0, 0, 0, 0, 0, 0, 2, 0x0A] // digit 10
    ));
    assert!(err(
        &KeyType::Numeric,
        &[0x03, 0x00, 0, 0, 0, 0, 0, 0, 0, 5, 1, 2] // digits cut short
    ));
    assert!(err(&KeyType::Jsonb, &[0x07])); // unknown tag
    assert!(err(&KeyType::Jsonb, &[0x04, 0, 0, 0, 2, 0x61])); // string overruns
                                                              // (a NUL inside a jsonb string is fine; only the length frames it)
    assert!(decode_value(&KeyType::Jsonb, &[0x04, 0, 0, 0, 2, 0x61, 0x00]).is_ok());
    assert!(err(&KeyType::Jsonb, &[0x04, 0, 0, 0, 2, 0xFF, 0xFE])); // string not UTF-8
    assert!(err(
        &KeyType::Jsonb,
        &[0x06, 0, 0, 0, 1, 0, 0, 0, 5, b'a'] // key overruns
    ));
    // Array: dims cut short; nullmap short; element short; second
    // element missing; and the empty array as a positive control.
    let arr = KeyType::Array(Box::new(KeyType::Int4));
    assert!(err(&arr, &[0x01, 0, 0, 0]));
    assert!(err(&arr, &[0x00, 0, 0, 0, 2]));
    assert!(err(&arr, &[0x00, 0, 0, 0, 1, 0x00, 0, 0, 0, 4, 0, 0]));
    assert!(err(&arr, &[0x00, 0, 0, 0, 2, 0x02, 0, 0, 0, 4, 0, 0]));
    assert_eq!(
        decode_value(&arr, &[0x00, 0, 0, 0, 0]),
        Ok(Value::Array(nucleus_codec::Array {
            dims: vec![],
            elems: vec![]
        }))
    );

    // Truncation at every offset for the framed types.
    let values = [
        (KeyType::Numeric, Value::Numeric(num("-123.450"))),
        (
            KeyType::Jsonb,
            Value::Jsonb(nucleus_codec::Jsonb::Object(vec![(
                "k".to_string(),
                nucleus_codec::Jsonb::Array(vec![nucleus_codec::Jsonb::Null]),
            )])),
        ),
        (
            arr.clone(),
            Value::Array(nucleus_codec::Array {
                dims: vec![nucleus_codec::ArrayDim { len: 2, lower: 0 }],
                elems: vec![Some(Value::Int4(1)), None],
            }),
        ),
    ];
    for (ty, v) in &values {
        let mut vb = Vec::new();
        encode_value(ty, v, &mut vb).unwrap_or_else(|e| panic!("encode_value: {e:?}"));
        for cut in 0..vb.len() {
            assert!(
                decode_value(ty, &vb[..cut]).is_err(),
                "{ty:?}: accepted cut {cut} of {vb:02x?}"
            );
        }
        assert!(decode_value(ty, &vb).is_ok(), "{ty:?}");
    }

    // Encode side: jsonb NaN, out-of-range digits, array-of-array type.
    use nucleus_codec::{Array, ArrayDim, Jsonb};
    let mut out = Vec::new();
    assert_eq!(
        Err(ValueError::Corrupt("jsonb number must be finite")),
        encode_value(
            &KeyType::Jsonb,
            &Value::Jsonb(Jsonb::Number(Numeric::NaN)),
            &mut out
        )
    );
    assert_eq!(
        Err(ValueError::Corrupt("numeric digit out of range")),
        encode_value(
            &KeyType::Numeric,
            &Value::Numeric(Numeric::Finite(Decimal {
                negative: false,
                digits: vec![1, 10],
                scale: 1,
            })),
            &mut out
        )
    );
    let bad_array = KeyType::Array(Box::new(KeyType::Array(Box::new(KeyType::Int4))));
    let v = Value::Array(Array {
        dims: vec![ArrayDim { len: 1, lower: 1 }],
        elems: vec![Some(Value::Array(Array {
            dims: vec![],
            elems: vec![],
        }))],
    });
    assert!(
        encode_value(&bad_array, &v, &mut out).is_err(),
        "array of array type must be refused"
    );
    assert!(decode_value(&bad_array, &[0x00, 0, 0, 0, 0]).is_err());
}
