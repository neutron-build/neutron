//! Regenerates the `hex` column of the golden corpus from the encoder.
//!
//! NOT part of `cargo test`: run it explicitly with
//! `cargo test -p nucleus-codec --test gen_corpus -- --ignored` after
//! changing the corpus literals or the encoding (a key-format change also
//! needs a spec edit and a format version bump, C-Q3s / C-K4).
//!
//! It only ever writes the `hex` column. The `rank` column is derived from
//! PostgreSQL semantics per the spec and must stay hand-written (card C-K1);
//! golden.rs cross-checks the two.

mod common;

use common::{columns_for, parse_row};
use std::fmt::Write as _;
use std::fs;
use std::path::PathBuf;

fn main() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/golden");
    let mut files: Vec<_> = fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("read {dir:?}: {e}"))
        .map(|e| e.unwrap_or_else(|e| panic!("readdir: {e}")).path())
        .filter(|p| p.extension().is_some_and(|x| x == "tsv"))
        .collect();
    files.sort();
    assert!(!files.is_empty(), "no golden files in {dir:?}");
    for path in files {
        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or_default()
            .to_string();
        let cols = columns_for(&stem);
        let text = fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
        let mut out = String::new();
        for line in text.lines() {
            if line.is_empty() || line.starts_with('#') {
                out.push_str(line);
                out.push('\n');
                continue;
            }
            let mut fields = line.split('\t');
            let _rank = fields.next().unwrap_or_else(|| panic!("{path:?}: no rank"));
            let lit = fields
                .next()
                .unwrap_or_else(|| panic!("{path:?}: no literal"))
                .to_string();
            let vals = parse_row(&stem, &lit);
            let mut buf = Vec::new();
            nucleus_codec::encode_key(&cols, &vals, &mut buf)
                .unwrap_or_else(|e| panic!("{path:?} {lit}: {e}"));
            let mut hex = String::with_capacity(2 * buf.len());
            for b in buf {
                write!(hex, "{b:02x}").unwrap_or_else(|e| panic!("fmt: {e}"));
            }
            out.push_str(&format!("{_rank}\t{lit}\t{hex}\n"));
        }
        fs::write(&path, out).unwrap_or_else(|e| panic!("write {path:?}: {e}"));
    }
}

#[test]
#[ignore = "maintainer tool: rewrites tests/golden/*.tsv in place"]
fn gen_corpus() {
    main();
}
