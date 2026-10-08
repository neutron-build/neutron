//! Golden corpus conformance (card C-K1 work item 3).
//!
//! For every `tests/golden/<type>.tsv` row:
//! - the parsed literal encodes to exactly `hex` (the corpus pins bytes);
//! - `decode_key(hex)` round-trips: re-encoding the decoded values
//!   reproduces the bytes (the decoded values are the canonical form);
//! - sorting the rows by encoded bytes follows the `rank` column, which is
//!   derived from PostgreSQL 17 semantics per C-Q3s, never from the
//!   encoder: byte order must be non-decreasing in rank, and equal ranks
//!   must have identical bytes (P-ORDER / P-EQ);
//! - under `DESC` the order reverses and under explicit `NULLS FIRST` /
//!   `NULLS LAST` the NULL placement moves (§2 defaults: ASC → NULLS LAST,
//!   DESC → NULLS FIRST).

mod common;

use common::{columns_for, parse_row};
use nucleus_codec::{decode_key, encode_key, Direction, KeyColumn, Nulls};
use std::path::PathBuf;

#[derive(Clone)]
struct Row {
    rank: i64,
    lit: String,
    hex: Vec<u8>,
    /// NULL in the first column (all columns, for scalar files).
    first_null: bool,
    /// NULL somewhere.
    any_null: bool,
}

fn load_files() -> Vec<(String, Vec<Row>)> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/golden");
    let mut paths: Vec<_> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("read {dir:?}: {e}"))
        .map(|e| e.unwrap_or_else(|e| panic!("readdir: {e}")).path())
        .filter(|p| p.extension().is_some_and(|x| x == "tsv"))
        .collect();
    paths.sort();
    paths
        .into_iter()
        .map(|path| {
            let stem = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or_default()
                .to_string();
            let text =
                std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
            let rows = text
                .lines()
                .filter(|l| !l.is_empty() && !l.starts_with('#'))
                .map(|line| {
                    let mut f = line.split('\t');
                    let rank: i64 = f
                        .next()
                        .unwrap_or_else(|| panic!("{line}: rank"))
                        .parse()
                        .unwrap_or_else(|e| panic!("{line}: {e}"));
                    let lit = f
                        .next()
                        .unwrap_or_else(|| panic!("{line}: literal"))
                        .to_string();
                    let hex = f
                        .next()
                        .unwrap_or_else(|| panic!("{line}: hex (run gen_corpus)"));
                    let bytes = (0..hex.len())
                        .step_by(2)
                        .map(|i| {
                            u8::from_str_radix(&hex[i..i + 2], 16)
                                .unwrap_or_else(|e| panic!("{line}: {e}"))
                        })
                        .collect();
                    let vals = parse_row(&stem, &lit);
                    Row {
                        rank,
                        lit,
                        hex: bytes,
                        first_null: vals[0].is_none(),
                        any_null: vals.iter().any(std::option::Option::is_none),
                    }
                })
                .collect();
            (stem, rows)
        })
        .collect()
}

fn cols(stem: &str, dir: Direction, nulls: Nulls) -> Vec<KeyColumn> {
    columns_for(stem)
        .into_iter()
        .map(|c| KeyColumn { dir, nulls, ..c })
        .collect()
}

fn enc(stem: &str, dir: Direction, nulls: Nulls, row: &Row) -> Vec<u8> {
    let vals = parse_row(stem, &row.lit);
    let mut buf = Vec::new();
    encode_key(&cols(stem, dir, nulls), &vals, &mut buf)
        .unwrap_or_else(|e| panic!("{stem} {}: {e}", row.lit));
    buf
}

#[test]
fn encode_matches_golden_and_round_trips() {
    for (stem, rows) in load_files() {
        assert!(rows.len() >= 25, "{stem}: {} rows < 25", rows.len());
        for row in &rows {
            // encode == pinned hex
            assert_eq!(
                row.hex,
                enc(&stem, Direction::Asc, Nulls::Last, row),
                "{stem} {}",
                row.lit
            );
            // decode -> canonical values -> identical bytes
            let decoded = decode_key(&cols(&stem, Direction::Asc, Nulls::Last), &row.hex)
                .unwrap_or_else(|e| panic!("{stem} {}: {e}", row.lit));
            let mut re = Vec::new();
            encode_key(&cols(&stem, Direction::Asc, Nulls::Last), &decoded, &mut re)
                .unwrap_or_else(|e| panic!("{stem} {}: {e}", row.lit));
            assert_eq!(row.hex, re, "{stem} {}: decode not canonical", row.lit);
        }
    }
}

#[test]
fn asc_order_follows_postgres_ranks() {
    for (stem, rows) in load_files() {
        let mut sorted = rows.clone();
        sorted.sort_by(|a, b| a.hex.cmp(&b.hex));
        for w in sorted.windows(2) {
            assert!(
                w[0].rank <= w[1].rank,
                "{stem}: {} (rank {}) > {} (rank {})",
                w[0].lit,
                w[0].rank,
                w[1].lit,
                w[1].rank
            );
            // P-EQ: SQL-equal <=> identical bytes, in both directions.
            assert_eq!(
                w[0].rank == w[1].rank,
                w[0].hex == w[1].hex,
                "{stem}: {} vs {}",
                w[0].lit,
                w[1].lit
            );
        }
    }
}

#[test]
fn desc_reverses_and_nulls_move() {
    for (stem, rows) in load_files() {
        // DESC (default NULLS FIRST): the exact reverse order — ranks
        // non-increasing — with the first-column NULLs moved to the front.
        let mut sorted: Vec<(Vec<u8>, &Row)> = rows
            .iter()
            .map(|r| (enc(&stem, Direction::Desc, Nulls::First, r), r))
            .collect();
        sorted.sort_by(|a, b| a.0.cmp(&b.0));
        let nulls_first = sorted.iter().take_while(|(_, r)| r.first_null).count();
        assert_eq!(
            rows.iter().filter(|r| r.first_null).count(),
            nulls_first,
            "{stem}: NULLs not first under DESC"
        );
        for w in sorted.windows(2) {
            assert!(
                w[0].1.rank >= w[1].1.rank,
                "{stem} DESC: {} (rank {}) < {} (rank {})",
                w[0].1.lit,
                w[0].1.rank,
                w[1].1.lit,
                w[1].1.rank
            );
        }

        // DESC NULLS LAST: NULLs at the back; fully non-null rows
        // non-increasing.
        let mut sorted: Vec<(Vec<u8>, &Row)> = rows
            .iter()
            .map(|r| (enc(&stem, Direction::Desc, Nulls::Last, r), r))
            .collect();
        sorted.sort_by(|a, b| a.0.cmp(&b.0));
        let nulls_last = sorted
            .iter()
            .rev()
            .take_while(|(_, r)| r.first_null)
            .count();
        assert_eq!(
            rows.iter().filter(|r| r.first_null).count(),
            nulls_last,
            "{stem}: NULLs not last under DESC NULLS LAST"
        );
        let non_null: Vec<i64> = sorted
            .iter()
            .filter(|(_, r)| !r.any_null)
            .map(|(_, r)| r.rank)
            .collect();
        for w in non_null.windows(2) {
            assert!(
                w[0] >= w[1],
                "{stem} DESC NULLS LAST: rank {} < {}",
                w[0],
                w[1]
            );
        }

        // ASC NULLS FIRST: NULLs moved to the front, non-nulls still
        // non-decreasing.
        let mut sorted: Vec<(Vec<u8>, &Row)> = rows
            .iter()
            .map(|r| (enc(&stem, Direction::Asc, Nulls::First, r), r))
            .collect();
        sorted.sort_by(|a, b| a.0.cmp(&b.0));
        let nulls_first = sorted.iter().take_while(|(_, r)| r.first_null).count();
        assert_eq!(
            rows.iter().filter(|r| r.first_null).count(),
            nulls_first,
            "{stem}: NULLs not first under NULLS FIRST"
        );
        let non_null: Vec<i64> = sorted
            .iter()
            .filter(|(_, r)| !r.any_null)
            .map(|(_, r)| r.rank)
            .collect();
        for w in non_null.windows(2) {
            assert!(
                w[0] <= w[1],
                "{stem} ASC NULLS FIRST: rank {} > {}",
                w[0],
                w[1]
            );
        }
    }
}
