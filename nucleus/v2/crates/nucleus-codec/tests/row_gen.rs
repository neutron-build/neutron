//! Regenerates the `hex` column of the row golden corpus from the
//! encoder.
//!
//! NOT part of `cargo test`: run it explicitly with
//! `cargo test -p nucleus-codec --test row_gen -- --ignored` after
//! changing the corpus literals or the row encoding (a row-format change
//! also needs a spec edit and a format version bump, C-T6 / C-K4).
//!
//! It only ever writes the `hex` column; `stem` and `literal` are
//! hand-written and pinned by row_golden.rs.

mod common;

use common::{parse_row, type_for};
use nucleus_codec::row::{encode_row, ColumnDef, Datum};
use std::fmt::Write as _;
use std::fs;
use std::path::PathBuf;

fn main() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/golden_row/rows.tsv");
    let text = fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
    let mut out = String::new();
    for line in text.lines() {
        if line.is_empty() || line.starts_with('#') {
            out.push_str(line);
            out.push('\n');
            continue;
        }
        let mut fields = line.split('\t');
        let stem = fields
            .next()
            .unwrap_or_else(|| panic!("{path:?}: no stem"))
            .to_string();
        let lit = fields
            .next()
            .unwrap_or_else(|| panic!("{path:?}: no literal"))
            .to_string();
        let vals = parse_row(&stem, &lit);
        assert_eq!(1, vals.len(), "{stem} {lit}");
        let col = ColumnDef {
            attnum: 1,
            name: "c".to_string(),
            ty: type_for(&stem),
            not_null: false,
            dropped: false,
            missing: None,
        };
        let live: Vec<Option<Datum>> = vec![vals.into_iter().next().flatten().map(Datum::Inline)];
        let bytes = encode_row(1, &[col], &live).unwrap_or_else(|e| panic!("{path:?} {lit}: {e}"));
        let mut hex = String::with_capacity(2 * bytes.len());
        for b in bytes {
            write!(hex, "{b:02x}").unwrap_or_else(|e| panic!("fmt: {e}"));
        }
        out.push_str(&format!("{stem}\t{lit}\t{hex}\n"));
    }
    fs::write(&path, out).unwrap_or_else(|e| panic!("write {path:?}: {e}"));
}

#[test]
#[ignore = "maintainer tool: rewrites tests/golden_row/rows.tsv in place"]
fn row_gen() {
    main();
}
