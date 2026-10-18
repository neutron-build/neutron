//! Schema evolution over the row bytes (card C-T6a2): the pure §3 DDL
//! helpers (`add_column`, `drop_column`, `set_not_null`) driving a1's
//! row encode/decode through the `TableSchema` wrappers, the §3 decode
//! rule across versions (old rows never rewritten), and the
//! `InvalidDdl` refusals.

use nucleus_codec::row::{row_header, ColumnDef, Datum, RowError, RowHeader, TableSchema};
use nucleus_codec::{Collation, KeyType, Value};

fn cd(name: &str, ty: KeyType) -> ColumnDef {
    ColumnDef {
        attnum: 0, // assigned by the caller
        name: name.to_string(),
        ty,
        not_null: false,
        not_null_since: 0,
        dropped: false,
        missing: None,
    }
}

fn sch(cols: Vec<ColumnDef>, pk: Vec<u16>) -> TableSchema {
    let cols: Vec<ColumnDef> = cols
        .into_iter()
        .enumerate()
        .map(|(i, mut c)| {
            c.attnum = i as u16 + 1;
            c
        })
        .collect();
    TableSchema::new(cols, pk).unwrap_or_else(|e| panic!("schema: {e:?}"))
}

fn i8v(v: i64) -> Option<Datum> {
    Some(Datum::Inline(Value::Int8(v)))
}

fn text(s: &str) -> Option<Datum> {
    Some(Datum::Inline(Value::Text(s.to_string())))
}

fn int4(v: i32) -> Option<Datum> {
    Some(Datum::Inline(Value::Int4(v)))
}

/// The card's scenario: v1 `(a int8 pk, b text)`; add `c` with missing
/// 7; add `d` without a default; drop `b`; add `e NOT NULL` missing
/// 'x'. The v1 row decodes under the v5 definition as `[a, 7, NULL,
/// 'x']`, and its header (and bytes) never change.
#[test]
fn evolution_decodes_old_rows() {
    let v1 = sch(
        vec![cd("a", KeyType::Int8), cd("b", KeyType::Text(Collation::C))],
        vec![1],
    );
    assert_eq!(1, v1.version);
    assert!(!v1.hidden_rowid);
    let row = v1
        .encode_row(&[i8v(1), text("one")])
        .unwrap_or_else(|e| panic!("encode: {e:?}"));
    assert_eq!(
        Ok(RowHeader {
            schema_version: 1,
            natts: 2
        }),
        row_header(&row)
    );

    let v2 = v1
        .add_column("c", KeyType::Int4, false, Some(Value::Int4(7)))
        .unwrap_or_else(|e| panic!("add c: {e:?}"));
    let v3 = v2
        .add_column("d", KeyType::Text(Collation::C), false, None)
        .unwrap_or_else(|e| panic!("add d: {e:?}"));
    let v4 = v3
        .drop_column(2)
        .unwrap_or_else(|e| panic!("drop b: {e:?}"));
    let v5 = v4
        .add_column(
            "e",
            KeyType::Text(Collation::C),
            true,
            Some(Value::Text("x".to_string())),
        )
        .unwrap_or_else(|e| panic!("add e: {e:?}"));
    assert_eq!(
        (2, 3, 4, 5),
        (v2.version, v3.version, v4.version, v5.version)
    );
    // Attnums are never reused: e is the 5th ever allocated.
    assert_eq!(5, v5.columns.last().map(|c| c.attnum).unwrap_or(0));

    // The §3 decode rule: for each live attnum of v5 in order, the row's
    // datum, or `missing` (else NULL) past the row's natts; dropped b
    // skipped. [a, 7, NULL, 'x'].
    let back = v5
        .decode_row(&row)
        .unwrap_or_else(|e| panic!("decode v1 row at v5: {e:?}"));
    assert_eq!(vec![i8v(1), int4(7), None, text("x")], back);

    // Lazy upgrade: no DDL rewrites rows — the v1 bytes and header are
    // unchanged, and v1 still decodes its own row.
    assert_eq!(
        Ok(RowHeader {
            schema_version: 1,
            natts: 2
        }),
        row_header(&row)
    );
    assert_eq!(
        vec![i8v(1), text("one")],
        v1.decode_row(&row).unwrap_or_else(|e| panic!("{e:?}"))
    );

    // live_columns: one entry per element of encode_row's `live`.
    assert_eq!(
        vec!["a", "c", "d", "e"],
        v5.live_columns()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>()
    );

    // A row written under v5 read at v1: VersionAhead (checked before
    // natts, so the wider row is reported as the version problem).
    let row5 = v5
        .encode_row(&[i8v(9), int4(7), text("dee"), text("x")])
        .unwrap_or_else(|e| panic!("encode v5: {e:?}"));
    assert_eq!(
        Err(RowError::VersionAhead { row: 5, schema: 1 }),
        v1.decode_row(&row5)
    );
    assert_eq!(
        4,
        v5.decode_row(&row5)
            .unwrap_or_else(|e| panic!("{e:?}"))
            .len()
    );

    // e is NOT NULL: a NULL for it is refused at encode.
    assert_eq!(
        Err(RowError::NotNull { attnum: 5 }),
        v5.encode_row(&[i8v(9), int4(7), text("dee"), None])
    );
}

/// DROP COLUMN clears `not_null` (§3): a dropped NOT NULL column is
/// written NULL on new rows and decodes — pre-drop rows too.
#[test]
fn dropped_not_null_column_writes_null() {
    let mut p = cd("p", KeyType::Int4);
    p.not_null = true;
    let s1 = sch(vec![p, cd("q", KeyType::Int4)], vec![]);
    assert!(s1.hidden_rowid); // no pk: hidden rowid
    let row = s1
        .encode_row(&[int4(1), None])
        .unwrap_or_else(|e| panic!("encode: {e:?}"));

    let s2 = s1.drop_column(1).unwrap_or_else(|e| panic!("drop: {e:?}"));
    assert_eq!(2, s2.version);
    assert!(s2.columns[0].dropped);
    assert!(!s2.columns[0].not_null, "DROP COLUMN clears not_null (§3)");
    // The dropped attnum is written NULL and decodes.
    let row2 = s2
        .encode_row(&[None])
        .unwrap_or_else(|e| panic!("encode after drop: {e:?}"));
    assert_eq!(
        vec![None],
        s2.decode_row(&row2).unwrap_or_else(|e| panic!("{e:?}"))
    );
    // The pre-drop row still decodes: p's old datum consumed, not
    // returned.
    assert_eq!(
        vec![None],
        s2.decode_row(&row).unwrap_or_else(|e| panic!("{e:?}"))
    );
}

/// A no-PK schema round-trips its def (hidden_rowid byte 1, npk 0) and
/// its rows.
#[test]
fn no_pk_schema_round_trips() {
    let s = sch(
        vec![cd("x", KeyType::Int4), cd("y", KeyType::Text(Collation::C))],
        vec![],
    );
    assert!(s.hidden_rowid);
    let bytes = s.encode().unwrap_or_else(|e| panic!("encode: {e:?}"));
    assert_eq!(1, bytes[6], "hidden_rowid byte");
    assert_eq!(&0u16.to_be_bytes(), &bytes[bytes.len() - 2..], "npk 0");
    assert_eq!(Ok(s.clone()), TableSchema::decode(&bytes));
    let row = s
        .encode_row(&[None, text("v")])
        .unwrap_or_else(|e| panic!("row: {e:?}"));
    assert_eq!(
        vec![None, text("v")],
        s.decode_row(&row).unwrap_or_else(|e| panic!("{e:?}"))
    );
}

/// The InvalidDdl refusals of the helpers (§3, §9).
#[test]
fn ddl_refusals() {
    let v1 = sch(
        vec![cd("a", KeyType::Int8), cd("b", KeyType::Text(Collation::C))],
        vec![1],
    );

    // ADD COLUMN: a duplicate live name is refused; a dropped column's
    // name may be reused (attnums are never reused, names are).
    assert_eq!(
        Err(RowError::InvalidDdl("duplicate column name")),
        v1.add_column("b", KeyType::Int4, false, None)
    );
    let dropped_b = v1.drop_column(2).unwrap_or_else(|e| panic!("{e:?}"));
    let reused = dropped_b
        .add_column("b", KeyType::Int4, false, None)
        .unwrap_or_else(|e| panic!("{e:?}"));
    assert_eq!(3, reused.columns.last().map(|c| c.attnum).unwrap_or(0));

    // A `missing` default must match the column type.
    assert_eq!(
        Err(RowError::TypeMismatch { attnum: 3 }),
        v1.add_column(
            "z",
            KeyType::Int4,
            false,
            Some(Value::Text("no".to_string()))
        )
    );
    // As must the type itself be encodable (no array of array).
    assert_eq!(
        Err(RowError::InvalidDdl("array of array type")),
        v1.add_column(
            "z",
            KeyType::Array(Box::new(KeyType::Array(Box::new(KeyType::Int4)))),
            false,
            None
        )
    );

    // DROP COLUMN: pk columns, unknown attnums and dropped attnums are
    // refused.
    assert_eq!(
        Err(RowError::InvalidDdl("cannot drop a primary-key column")),
        v1.drop_column(1)
    );
    assert_eq!(
        Err(RowError::InvalidDdl("no such column")),
        v1.drop_column(0)
    );
    assert_eq!(
        Err(RowError::InvalidDdl("no such column")),
        v1.drop_column(99)
    );
    assert_eq!(
        Err(RowError::InvalidDdl("column already dropped")),
        dropped_b.drop_column(2)
    );

    // SET / DROP NOT NULL.
    let v2 = v1.set_not_null(2, true).unwrap_or_else(|e| panic!("{e:?}"));
    assert_eq!(2, v2.version);
    assert!(v2.columns[1].not_null);
    assert_eq!(
        Err(RowError::NotNull { attnum: 2 }),
        v2.encode_row(&[i8v(1), None])
    );
    let v3 = v2
        .set_not_null(2, false)
        .unwrap_or_else(|e| panic!("{e:?}"));
    assert_eq!(3, v3.version);
    assert!(!v3.columns[1].not_null);
    assert!(v3.encode_row(&[i8v(1), None]).is_ok());
    assert_eq!(
        Err(RowError::InvalidDdl("no such column")),
        v1.set_not_null(0, true)
    );
    assert_eq!(
        Err(RowError::InvalidDdl("no such column")),
        v1.set_not_null(99, true)
    );
    assert_eq!(
        Err(RowError::InvalidDdl("column already dropped")),
        dropped_b.set_not_null(2, true)
    );

    // Version overflow: no helper panics at u32::MAX.
    let mut maxed = v1.clone();
    maxed.version = u32::MAX;
    assert_eq!(
        Err(RowError::InvalidDdl("version overflow")),
        maxed.add_column("n", KeyType::Int4, false, None)
    );
    assert_eq!(
        Err(RowError::InvalidDdl("version overflow")),
        maxed.drop_column(2)
    );
    assert_eq!(
        Err(RowError::InvalidDdl("version overflow")),
        maxed.set_not_null(2, true)
    );
}
