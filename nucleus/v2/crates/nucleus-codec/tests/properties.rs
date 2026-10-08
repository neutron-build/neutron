//! Encoding property tests against the independent reference comparator
//! (card C-K2, work item 3): P-ORDER, P-EQ, P-HASH and P-PREFIX (C-Q3s §1)
//! for every key type, plus composite keys of 1-4 columns, DESC reversal,
//! decode round-trips and a cross-check of the reference itself against the
//! golden corpus ranks.
//!
//! 10_000 cases per property by default; `NUCLEUS_CODEC_CASES` overrides
//! (the release acceptance run uses 200_000).

mod common;
mod reference;

use common::{columns_for, parse_row};
use nucleus_codec::{decode_key, encode_key, hash_value, KeyColumn, Nulls, Value};
use proptest::prelude::*;
use reference::{
    column_cmp, column_with_optional_pair, composite_key_pair, key_cmp,
    key_type_with_optional_pair, key_type_with_pair, key_type_with_value, sql_cmp,
};
use std::cmp::Ordering;
use std::path::PathBuf;

fn cfg() -> ProptestConfig {
    let cases = std::env::var("NUCLEUS_CODEC_CASES")
        .ok()
        .map(|s| {
            s.parse::<u32>()
                .unwrap_or_else(|e| panic!("bad NUCLEUS_CODEC_CASES {s:?}: {e}"))
        })
        .unwrap_or(10_000);
    let mut config = ProptestConfig::with_cases(cases);
    // The corpus and the fuzz target pin bytes; failing seeds would add
    // files without adding information.
    config.failure_persistence = None;
    config
}

fn sign(o: Ordering) -> i32 {
    match o {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    }
}

fn enc(cols: &[KeyColumn], vals: &[Option<Value>]) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_key(cols, vals, &mut buf).unwrap_or_else(|e| panic!("encode: {e}"));
    buf
}

/// One column, one value.
fn enc1(col: &KeyColumn, val: &Option<Value>) -> Vec<u8> {
    enc(std::slice::from_ref(col), std::slice::from_ref(val))
}

proptest! {
    #![proptest_config(cfg())]

    /// P-ORDER (§1), one ASC NULLS LAST column, non-NULL values:
    /// `memcmp(enc(a), enc(b))` has the sign of the SQL comparison.
    #[test]
    fn p_order((ty, a, b) in key_type_with_pair()) {
        let col = KeyColumn::asc(ty.clone());
        let ka = enc1(&col, &Some(a.clone()));
        let kb = enc1(&col, &Some(b.clone()));
        prop_assert_eq!(sign(ka.cmp(&kb)), sign(sql_cmp(&ty, &a, &b)));
    }

    /// P-ORDER with NULLs, direction and NULLS placement mixed in.
    #[test]
    fn p_order_column((col, a, b) in column_with_optional_pair()) {
        let ka = enc1(&col, &a);
        let kb = enc1(&col, &b);
        prop_assert_eq!(sign(ka.cmp(&kb)), sign(column_cmp(&col, &a, &b)));
    }

    /// P-EQ (§1): `a = b` under the SQL comparison iff the encodings are
    /// byte-identical, in both directions.
    #[test]
    fn p_eq_iff_identical_bytes((ty, a, b) in key_type_with_pair()) {
        let col = KeyColumn::asc(ty.clone());
        let ka = enc1(&col, &Some(a.clone()));
        let kb = enc1(&col, &Some(b.clone()));
        prop_assert_eq!(sql_cmp(&ty, &a, &b) == Ordering::Equal, ka == kb);
    }

    /// P-EQ over full columns (NULL compares equal to NULL).
    #[test]
    fn p_eq_column_iff_identical_bytes((col, a, b) in column_with_optional_pair()) {
        let ka = enc1(&col, &a);
        let kb = enc1(&col, &b);
        prop_assert_eq!(column_cmp(&col, &a, &b) == Ordering::Equal, ka == kb);
    }

    /// P-HASH (§1, §8): equal values hash equal. The pair strategy already
    /// produces equal values (identical and representation variants), so
    /// non-equal pairs simply skip.
    #[test]
    fn p_hash_equal_values_hash_equal((ty, a, b) in key_type_with_pair()) {
        if sql_cmp(&ty, &a, &b) == Ordering::Equal {
            let ha = hash_value(&ty, &a).unwrap_or_else(|e| panic!("hash: {e}"));
            let hb = hash_value(&ty, &b).unwrap_or_else(|e| panic!("hash: {e}"));
            prop_assert_eq!(ha, hb);
        }
    }

    /// P-PREFIX (§1, §2): no column encoding is a proper prefix of another
    /// of the same column type.
    #[test]
    fn p_prefix_free((ty, a, b) in key_type_with_optional_pair()) {
        let col = KeyColumn::asc(ty.clone());
        let ka = enc1(&col, &a);
        let kb = enc1(&col, &b);
        if ka != kb {
            prop_assert!(!(ka.len() < kb.len() && kb.starts_with(&ka)));
            prop_assert!(!(kb.len() < ka.len() && ka.starts_with(&kb)));
        }
    }

    /// §8.1: decoding a key yields values that re-encode to the same bytes.
    #[test]
    fn p_decode_reencodes_identically((ty, v) in key_type_with_value()) {
        let col = KeyColumn::asc(ty.clone());
        let k = enc1(&col, &Some(v));
        let decoded = decode_key(std::slice::from_ref(&col), &k)
            .unwrap_or_else(|e| panic!("decode: {e}"));
        let val = decoded
            .into_iter()
            .next()
            .unwrap_or_else(|| panic!("decode returned no values"));
        prop_assert_eq!(enc1(&col, &val), k);
    }

    /// §2: DESC reverses the column order, for the PostgreSQL default pair
    /// (ASC NULLS LAST vs DESC NULLS FIRST) and for the explicit overrides
    /// (ASC NULLS FIRST vs DESC NULLS LAST).
    #[test]
    fn p_desc_reverses_order((ty, a, b) in key_type_with_optional_pair()) {
        let asc_last = KeyColumn::asc(ty.clone());
        let desc_first = KeyColumn::desc(ty.clone());
        let asc_first = asc_last.clone().with_nulls(Nulls::First);
        let desc_last = desc_first.clone().with_nulls(Nulls::Last);
        for (asc, desc) in [(asc_last, desc_first), (asc_first, desc_last)] {
            let aa = enc1(&asc, &a);
            let ab = enc1(&asc, &b);
            let da = enc1(&desc, &a);
            let db = enc1(&desc, &b);
            prop_assert_eq!(sign(da.cmp(&db)), sign(aa.cmp(&ab).reverse()));
        }
    }

    /// §3: composite byte order is the lexicographic column order; equal
    /// keys iff identical bytes; decode round-trips; and encoding the first
    /// k columns yields a prefix of the full key (leading-column range
    /// scans).
    #[test]
    fn p_composite((cols, va, vb) in composite_key_pair()) {
        let ka = enc(&cols, &va);
        let kb = enc(&cols, &vb);
        prop_assert_eq!(sign(ka.cmp(&kb)), sign(key_cmp(&cols, &va, &vb)));
        prop_assert_eq!(key_cmp(&cols, &va, &vb) == Ordering::Equal, ka == kb);

        let decoded = decode_key(&cols, &ka).unwrap_or_else(|e| panic!("decode: {e}"));
        prop_assert_eq!(enc(&cols, &decoded), ka.clone());

        for k in 1..cols.len() {
            let prefix = enc(&cols[..k], &va[..k]);
            prop_assert!(ka.starts_with(&prefix));
        }
    }
}

/// The reference comparator must agree with the hand-written golden corpus
/// ranks (C-K1): for every pair of rows, the sign of `key_cmp` equals the
/// sign of the rank difference, so equal ranks compare equal. This checks
/// the oracle itself, independently of the encoder.
#[test]
fn reference_matches_golden_ranks() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/golden");
    let mut paths: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("read {dir:?}: {e}"))
        .map(|e| e.unwrap_or_else(|e| panic!("readdir: {e}")).path())
        .filter(|p| p.extension().is_some_and(|x| x == "tsv"))
        .collect();
    paths.sort();
    assert!(paths.len() >= 18, "missing golden files in {dir:?}");
    for path in paths {
        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or_default()
            .to_string();
        let cols = columns_for(&stem);
        let rows: Vec<(i64, String)> = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {path:?}: {e}"))
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
                (rank, lit)
            })
            .collect();
        assert!(rows.len() >= 25, "{stem}: {} rows < 25", rows.len());
        for i in 0..rows.len() {
            for j in (i + 1)..rows.len() {
                let va = parse_row(&stem, &rows[i].1);
                let vb = parse_row(&stem, &rows[j].1);
                assert_eq!(
                    sign(key_cmp(&cols, &va, &vb)),
                    sign(rows[i].0.cmp(&rows[j].0)),
                    "{stem}: {} (rank {}) vs {} (rank {})",
                    rows[i].1,
                    rows[i].0,
                    rows[j].1,
                    rows[j].0
                );
            }
        }
    }
}
