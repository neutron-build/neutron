//! Regenerates the `hex` column of the golden corpora from the
//! encoders.
//!
//! NOT part of `cargo test`: run it explicitly with
//! `cargo test -p nucleus-codec --test row_gen -- --ignored` after
//! changing the corpus literals or an encoding (a format change also
//! needs a spec edit and a format version bump, C-T6 / C-K4).
//!
//! It only ever writes the `hex` column; every other field is
//! hand-written and pinned by the tests:
//! - `golden_row/rows.tsv`: stem, literal (single-column rows, C-T6a1);
//! - `golden_row/wide.tsv`: natts, literal, dropped attnum — rows with
//!   more than 8 columns pinning the second nullmap byte (C-T6a2 F1);
//! - `golden_row/defs.tsv`: name, description — one def per §3.1 tag
//!   plus the flag/pk/hidden-rowid shapes. The schema table mirrors
//!   `row_defs.rs::def_cases`.

mod common;

use common::{parse_row, type_for};
use nucleus_codec::row::{encode_row, ColumnDef, Datum, TableSchema};
use nucleus_codec::{Collation, KeyType, Value};
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(2 * bytes.len());
    for b in bytes {
        write!(out, "{b:02x}").unwrap_or_else(|e| panic!("fmt: {e}"));
    }
    out
}

fn rewrite(path: &Path, line: impl Fn(&str) -> String) {
    let text = fs::read_to_string(path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
    let mut out = String::new();
    for line_in in text.lines() {
        if line_in.is_empty() || line_in.starts_with('#') {
            out.push_str(line_in);
            out.push('\n');
            continue;
        }
        out.push_str(&line(line_in));
        out.push('\n');
    }
    fs::write(path, out).unwrap_or_else(|e| panic!("write {path:?}: {e}"));
}

fn corpus(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden_row")
        .join(name)
}

/// The single-column rows of C-T6a1: stem, literal, hex.
fn gen_rows() {
    let path = corpus("rows.tsv");
    rewrite(&path, |line| {
        let mut fields = line.split('\t');
        let stem = fields.next().unwrap_or_else(|| panic!("{line}: stem"));
        let lit = fields.next().unwrap_or_else(|| panic!("{line}: literal"));
        let vals = parse_row(stem, lit);
        assert_eq!(1, vals.len(), "{stem} {lit}");
        let col = ColumnDef {
            attnum: 1,
            name: "c".to_string(),
            ty: type_for(stem),
            not_null: false,
            dropped: false,
            missing: None,
        };
        let live: Vec<Option<Datum>> = vec![vals.into_iter().next().flatten().map(Datum::Inline)];
        let bytes = encode_row(1, &[col], &live).unwrap_or_else(|e| panic!("{path:?} {lit}: {e}"));
        format!("{stem}\t{lit}\t{}", hex(&bytes))
    });
}

/// The wide rows of C-T6a2 (gate follow-up F1): natts, literal, hex,
/// dropped attnum.
fn gen_wide() {
    let path = corpus("wide.tsv");
    rewrite(&path, |line| {
        let mut fields = line.split('\t');
        let stem = fields.next().unwrap_or_else(|| panic!("{line}: natts"));
        let lit = fields.next().unwrap_or_else(|| panic!("{line}: literal"));
        // Before the first run the line is (stem, literal, dropped);
        // afterwards the hex column sits between literal and dropped.
        let rest: Vec<&str> = fields.collect();
        let dropped_field = match rest.as_slice() {
            [dropped] => *dropped,
            [_hex, dropped] => *dropped,
            _ => panic!("{line}: expected (literal, [hex], dropped)"),
        };
        let (cols, live) = wide_row(stem, lit, dropped_field);
        let bytes = encode_row(1, &cols, &live).unwrap_or_else(|e| panic!("{path:?} {stem}: {e}"));
        format!("{stem}\t{lit}\t{}\t{dropped_field}", hex(&bytes))
    });
}

/// The definition corpus of C-T6a2: name, description, hex.
fn gen_defs() {
    let path = corpus("defs.tsv");
    let cases = def_cases();
    rewrite(&path, |line| {
        let mut fields = line.split('\t');
        let name = fields.next().unwrap_or_else(|| panic!("{line}: name"));
        let desc = fields
            .next()
            .unwrap_or_else(|| panic!("{line}: description"));
        let schema = cases
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, s)| s)
            .unwrap_or_else(|| panic!("{line}: no such case"));
        let bytes = schema.encode().unwrap_or_else(|e| panic!("{line}: {e}"));
        format!("{name}\t{desc}\t{}", hex(&bytes))
    });
}

fn wide_row(stem: &str, lit: &str, dropped_field: &str) -> (Vec<ColumnDef>, Vec<Option<Datum>>) {
    let n: usize = stem["wide".len()..]
        .parse()
        .unwrap_or_else(|e| panic!("{stem}: {e}"));
    let dropped: usize = dropped_field
        .parse()
        .unwrap_or_else(|e| panic!("{stem}: dropped {dropped_field}: {e}"));
    let entries: Vec<&str> = lit.split(',').collect();
    assert_eq!(n, entries.len(), "{lit}: {n} entries expected");
    assert!(dropped > 8, "{stem}: the dropped attnum must be above 8");
    let cols: Vec<ColumnDef> = (0..n)
        .map(|i| ColumnDef {
            attnum: i as u16 + 1,
            name: format!("c{}", i + 1),
            ty: KeyType::Bool,
            not_null: false,
            dropped: i + 1 == dropped,
            missing: None,
        })
        .collect();
    let live: Vec<Option<Datum>> = entries
        .iter()
        .enumerate()
        .filter(|(i, _)| i + 1 != dropped)
        .map(|(_, e)| match *e {
            "t" => Some(Datum::Inline(Value::Bool(true))),
            "f" => Some(Datum::Inline(Value::Bool(false))),
            "NULL" => None,
            other => panic!("{lit}: bad wide entry {other}"),
        })
        .collect();
    (cols, live)
}

fn cd(attnum: u16, name: &str, ty: KeyType) -> ColumnDef {
    ColumnDef {
        attnum,
        name: name.to_string(),
        ty,
        not_null: false,
        dropped: false,
        missing: None,
    }
}

fn sch(columns: Vec<ColumnDef>, pk: Vec<u16>) -> TableSchema {
    TableSchema::new(columns, pk).unwrap_or_else(|e| panic!("case schema: {e:?}"))
}

/// Mirrors `row_defs.rs::def_cases` (the pinned hex makes them fail
/// loudly if they drift apart).
fn def_cases() -> Vec<(&'static str, TableSchema)> {
    let one = |ty: KeyType| sch(vec![cd(1, "c", ty)], vec![1]);
    let mut flagged = cd(1, "c", KeyType::Int4);
    flagged.not_null = true;
    let mut dropped = cd(1, "c", KeyType::Int4);
    dropped.dropped = true; // DDL state: drop_column clears not_null (§3)
    let mut missing = cd(1, "c", KeyType::Int4);
    missing.missing = Some(Value::Int4(7));
    let mut missing_text = cd(1, "c", KeyType::Text(Collation::C));
    missing_text.missing = Some(Value::Text("héllo".to_string()));
    vec![
        ("bool", one(KeyType::Bool)),
        ("int2", one(KeyType::Int2)),
        ("int4", one(KeyType::Int4)),
        ("int8", one(KeyType::Int8)),
        ("float4", one(KeyType::Float4)),
        ("float8", one(KeyType::Float8)),
        ("numeric", one(KeyType::Numeric)),
        ("text", one(KeyType::Text(Collation::C))),
        ("bytea", one(KeyType::Bytea)),
        ("date", one(KeyType::Date)),
        ("time", one(KeyType::Time)),
        ("timestamp", one(KeyType::Timestamp)),
        ("timestamptz", one(KeyType::TimestampTz)),
        ("interval", one(KeyType::Interval)),
        ("uuid", one(KeyType::Uuid)),
        ("jsonb", one(KeyType::Jsonb)),
        ("array_int4", one(KeyType::Array(Box::new(KeyType::Int4)))),
        (
            "array_text",
            one(KeyType::Array(Box::new(KeyType::Text(Collation::C)))),
        ),
        ("not_null", sch(vec![flagged], vec![1])),
        ("dropped", sch(vec![dropped], vec![])),
        ("missing_int4", sch(vec![missing], vec![1])),
        ("missing_text", sch(vec![missing_text], vec![1])),
        (
            "two_pk",
            sch(
                vec![
                    cd(1, "a", KeyType::Int8),
                    cd(2, "b", KeyType::Text(Collation::C)),
                ],
                vec![1, 2],
            ),
        ),
        ("hidden_rowid", sch(vec![cd(1, "a", KeyType::Int4)], vec![])),
        (
            "long_name",
            sch(vec![cd(1, "renamed_by_ddl_column", KeyType::Int4)], vec![1]),
        ),
        ("empty", sch(vec![], vec![])),
    ]
}

fn main() {
    gen_rows();
    gen_wide();
    gen_defs();
}

#[test]
#[ignore = "maintainer tool: rewrites the golden corpora in place"]
fn row_gen() {
    main();
}
