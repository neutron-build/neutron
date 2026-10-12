//! Row-format golden corpus conformance (card C-T6a1): every line of
//! `tests/golden_row/rows.tsv` is a version-1 row of one nullable column
//! of the stem's type; the pinned `hex` is exactly what `encode_row`
//! produces (C-T6 §2), and it decodes back to the literal's value.

mod common;

use common::{parse_row, type_for};
use nucleus_codec::row::{decode_row, decode_row_raw, encode_row, value_repr_eq, ColumnDef, Datum};
use nucleus_codec::{KeyType, Value};
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

struct WideLine {
    natts: usize,
    lit: String,
    hex: Vec<u8>,
    dropped: usize,
}

fn load_wide() -> Vec<WideLine> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/golden_row/wide.tsv");
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
    text.lines()
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(|line| {
            let mut f = line.split('\t');
            let stem = f
                .next()
                .unwrap_or_else(|| panic!("{line}: natts"))
                .to_string();
            let lit = f
                .next()
                .unwrap_or_else(|| panic!("{line}: literal"))
                .to_string();
            let hex = f
                .next()
                .unwrap_or_else(|| panic!("{line}: hex (run row_gen)"));
            let dropped = f.next().unwrap_or_else(|| panic!("{line}: dropped attnum"));
            let bytes = (0..hex.len())
                .step_by(2)
                .map(|i| {
                    u8::from_str_radix(&hex[i..i + 2], 16).unwrap_or_else(|e| panic!("{line}: {e}"))
                })
                .collect();
            WideLine {
                natts: stem["wide".len()..]
                    .parse()
                    .unwrap_or_else(|e| panic!("{stem}: {e}")),
                lit,
                hex: bytes,
                dropped: dropped
                    .parse()
                    .unwrap_or_else(|e| panic!("{line}: dropped: {e}")),
            }
        })
        .collect()
}

/// The wide rows of the corpus: (columns, live values) of one line —
/// bool columns, the dropped attnum (always > 8) excluded from live.
fn wide_row(line: &WideLine) -> (Vec<ColumnDef>, Vec<Option<Datum>>) {
    let entries: Vec<&str> = line.lit.split(',').collect();
    assert_eq!(line.natts, entries.len(), "{}", line.lit);
    let cols: Vec<ColumnDef> = (0..line.natts)
        .map(|i| ColumnDef {
            attnum: i as u16 + 1,
            name: format!("c{}", i + 1),
            ty: KeyType::Bool,
            not_null: false,
            dropped: i + 1 == line.dropped,
            missing: None,
        })
        .collect();
    let live: Vec<Option<Datum>> = entries
        .iter()
        .enumerate()
        .filter(|(i, _)| i + 1 != line.dropped)
        .map(|(_, e)| match *e {
            "t" => Some(Datum::Inline(Value::Bool(true))),
            "f" => Some(Datum::Inline(Value::Bool(false))),
            "NULL" => None,
            other => panic!("{}: bad wide entry {other}", line.lit),
        })
        .collect();
    (cols, live)
}

/// Wide-row golden conformance (card C-T6a2, C-T6a1 gate follow-up
/// F1): rows of 9, 16 and 17 bool columns — NULLs at attnums 8, 9, 16
/// and 17 and a dropped attnum above 8 — pin the nullmap's second and
/// third bytes (byte `(a-1)/8`, LSB first, C-T6 §2).
#[test]
fn wide_rows_golden() {
    let lines = load_wide();
    assert!(!lines.is_empty(), "no wide rows in the corpus");
    let stems: Vec<usize> = lines.iter().map(|l| l.natts).collect();
    assert_eq!(
        vec![9, 16, 17],
        stems,
        "the corpus must pin natts 9, 16 and 17"
    );
    for line in &lines {
        assert!(
            line.natts > 8,
            "natts {} does not cross a nullmap byte",
            line.natts
        );
        assert!(line.dropped > 8, "the dropped attnum must be above 8");
        // NULLs at the boundary attnums the line covers.
        for a in [8usize, 9, 16, 17] {
            if a <= line.natts && a != line.dropped {
                assert_eq!(
                    "NULL",
                    line.lit
                        .split(',')
                        .nth(a - 1)
                        .unwrap_or_else(|| panic!("{}", line.lit)),
                    "attnum {a} must be NULL in {}",
                    line.lit
                );
            }
        }
        let (cols, live) = wide_row(line);
        let bytes =
            encode_row(1, &cols, &live).unwrap_or_else(|e| panic!("natts {}: {e}", line.natts));
        assert_eq!(
            line.hex, bytes,
            "natts {}: bytes drifted (row_gen rewrites only the hex column)",
            line.natts
        );
        assert_eq!(Ok(live.clone()), decode_row(1, &cols, &line.hex));
        let (header, raw) =
            decode_row_raw(&line.hex).unwrap_or_else(|e| panic!("natts {}: {e}", line.natts));
        assert_eq!(1, header.schema_version);
        assert_eq!(line.natts, usize::from(header.natts));
        assert_eq!(line.natts, raw.len());
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
