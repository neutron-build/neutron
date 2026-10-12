//! Row-format golden corpus conformance (card C-T6a1): every line of
//! `tests/golden_row/rows.tsv` is a version-1 row of one nullable column
//! of the stem's type; the pinned `hex` is exactly what `encode_row`
//! produces (C-T6 §2), and it decodes back to the literal's value.

mod common;

use common::{parse_row, type_for};
use nucleus_codec::row::{decode_row, encode_row, value_repr_eq, ColumnDef, Datum};
use std::collections::BTreeMap;
use std::path::PathBuf;

struct Line {
    stem: String,
    lit: String,
    hex: Vec<u8>,
}

fn load() -> Vec<Line> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/golden_row/rows.tsv");
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
    text.lines()
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(|line| {
            let mut f = line.split('\t');
            let stem = f
                .next()
                .unwrap_or_else(|| panic!("{line}: stem"))
                .to_string();
            let lit = f
                .next()
                .unwrap_or_else(|| panic!("{line}: literal"))
                .to_string();
            let hex = f
                .next()
                .unwrap_or_else(|| panic!("{line}: hex (run row_gen)"));
            let bytes = (0..hex.len())
                .step_by(2)
                .map(|i| {
                    u8::from_str_radix(&hex[i..i + 2], 16).unwrap_or_else(|e| panic!("{line}: {e}"))
                })
                .collect();
            Line {
                stem,
                lit,
                hex: bytes,
            }
        })
        .collect()
}

fn col(ty: nucleus_codec::KeyType) -> ColumnDef {
    ColumnDef {
        attnum: 1,
        name: "c".to_string(),
        ty,
        not_null: false,
        dropped: false,
        missing: None,
    }
}

#[test]
fn row_golden() {
    let lines = load();
    assert!(!lines.is_empty(), "no rows in the corpus");

    // Every type of the key corpus (composite aside, which is not a
    // single-column row), with >= 3 literals plus a NULL each.
    let mut counts: BTreeMap<String, (usize, usize)> = BTreeMap::new();
    for line in &lines {
        let (nonnull, nulls) = counts.entry(line.stem.clone()).or_insert((0, 0));
        if line.lit == "NULL" {
            *nulls += 1;
        } else {
            *nonnull += 1;
        }
    }
    let expected: Vec<&str> = vec![
        "array",
        "bool",
        "bytea",
        "date",
        "float4",
        "float8",
        "int2",
        "int4",
        "int8",
        "interval",
        "jsonb",
        "numeric",
        "text",
        "time",
        "timestamp",
        "timestamptz",
        "uuid",
        "varchar",
    ];
    let stems: Vec<String> = counts.keys().cloned().collect();
    assert_eq!(
        expected.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
        stems,
        "the corpus must cover exactly the single-column golden types"
    );
    for (stem, (nonnull, nulls)) in &counts {
        assert!(*nonnull >= 3, "{stem}: {nonnull} literals < 3");
        assert!(*nulls >= 1, "{stem}: no NULL row");
    }

    for line in &lines {
        let vals = parse_row(&line.stem, &line.lit);
        assert_eq!(1, vals.len(), "{} {}", line.stem, line.lit);
        let c = col(type_for(&line.stem));
        let live: Vec<Option<Datum>> = vec![vals.into_iter().next().flatten().map(Datum::Inline)];
        let bytes = encode_row(1, std::slice::from_ref(&c), &live)
            .unwrap_or_else(|e| panic!("{} {}: {e}", line.stem, line.lit));
        assert_eq!(
            line.hex, bytes,
            "{} {}: bytes drifted (row_gen rewrites only the hex column)",
            line.stem, line.lit
        );
        let back = decode_row(1, &[c], &line.hex)
            .unwrap_or_else(|e| panic!("{} {}: {e}", line.stem, line.lit));
        assert_eq!(1, back.len(), "{} {}", line.stem, line.lit);
        match (&back[0], &live[0]) {
            (None, None) => {}
            (Some(Datum::Inline(got)), Some(Datum::Inline(want))) => assert!(
                value_repr_eq(got, want),
                "{} {}: decoded {got:?} != {want:?}",
                line.stem,
                line.lit
            ),
            _ => panic!("{} {}: datum shape changed", line.stem, line.lit),
        }
    }
}
