//! C-T2b unit tests: the shared row-lock table's grant / holders /
//! keys_of / release round trip, per-seq release, dedup, determinism and
//! the exclusive-mode refusal (§6); the relation-mode conflict matrix
//! against its own literal (§6, "PostgreSQL's table-level conflict
//! table"). Each test names the mutant it must kill.

mod locks_support;

use std::collections::BTreeSet;

use locks_support::ok;
use nucleus_txn::locks::{RelLockMode, RowLockTable, REL_LOCK_CONFLICTS};
use nucleus_txn::write::RowLocks as _;
use nucleus_txn::{RowLockMode, TxnId};

fn id(n: u64) -> TxnId {
    TxnId { epoch: 1, n }
}

/// grant / holders / keys_of / release round trip (§6).
/// Mutant: `release` drops every acquisition of the txn regardless of seq
/// (then the seq-1 KEY SHARE below disappears too and the first assert
/// fails).
#[test]
fn row_table_round_trip_and_per_seq_release() {
    let t = RowLockTable::new();
    ok(t.grant(b"/t/1/k", id(1), RowLockMode::KeyShare, 1, None));
    ok(t.grant(b"/t/1/k", id(1), RowLockMode::Share, 3, None));
    ok(t.grant(b"/t/1/other", id(2), RowLockMode::Share, 2, Some(8)));
    assert_eq!(
        t.holders(b"/t/1/k"),
        vec![
            (id(1), RowLockMode::KeyShare, 1),
            (id(1), RowLockMode::Share, 3)
        ],
        "one acquisition per (txn, mode, seq), in grant order"
    );
    assert_eq!(
        t.holders(b"/t/1/other"),
        vec![(id(2), RowLockMode::Share, 2)]
    );
    assert_eq!(
        t.holders(b"/t/1/none"),
        Vec::<(TxnId, RowLockMode, u32)>::new()
    );

    // keys_of from 0 names every key the txn holds; from 2 only keys with
    // an acquisition at seq >= 2.
    let keys: BTreeSet<Vec<u8>> = t.keys_of(id(1), 0).into_iter().map(|(k, _)| k).collect();
    assert_eq!(keys, BTreeSet::from([b"/t/1/k".to_vec()]));
    assert_eq!(t.keys_of(id(1), 2).len(), 1, "the seq-3 SHARE is >= 2");

    // ROLLBACK TO 2 (§5.5): the seq-1 KEY SHARE survives, the seq-3 SHARE
    // is dropped.
    t.release(b"/t/1/k", id(1), 2);
    assert_eq!(
        t.holders(b"/t/1/k"),
        vec![(id(1), RowLockMode::KeyShare, 1)],
        "a KEY SHARE at seq 1 survives ROLLBACK TO 2"
    );
    assert_eq!(t.keys_of(id(1), 2).len(), 0, "no acquisition at seq >= 2");
    assert_eq!(
        t.keys_of(id(1), 0).len(),
        1,
        "the survivor still names the key"
    );

    // The latch prefix travels with the acquisition (§5.0, seed 46).
    let (_, prefix) = t.keys_of(id(2), 0).pop().expect("entry");
    assert_eq!(prefix, Some(8));

    // Full release empties the per-key and per-txn entries.
    t.release(b"/t/1/k", id(1), 0);
    assert!(t.holders(b"/t/1/k").is_empty());
    assert!(t.keys_of(id(1), 0).is_empty());
    t.release(b"/t/1/other", id(2), 0);
    assert!(t.holders(b"/t/1/other").is_empty());
}

/// An identical (txn, mode, seq) re-grant is deduplicated (a §5.1 retry
/// that re-runs the grant must not double the acquisition).
#[test]
fn row_table_dedups_identical_acquisition() {
    let t = RowLockTable::new();
    ok(t.grant(b"/t/1/k", id(7), RowLockMode::Share, 4, None));
    ok(t.grant(b"/t/1/k", id(7), RowLockMode::Share, 4, None));
    assert_eq!(t.holders(b"/t/1/k"), vec![(id(7), RowLockMode::Share, 4)]);
    t.release(b"/t/1/k", id(7), 4);
    assert!(
        t.holders(b"/t/1/k").is_empty(),
        "one release drops the one acquisition"
    );
}

/// `grant` accepts KEY SHARE and SHARE only (§2.1: exclusive modes live in
/// intents).
#[test]
fn row_table_rejects_exclusive_modes() {
    let t = RowLockTable::new();
    for mode in [RowLockMode::NoKeyUpdate, RowLockMode::Update] {
        assert!(
            t.grant(b"/t/1/k", id(1), mode, 1, None).is_err(),
            "{mode:?} must be refused"
        );
    }
    assert!(t.holders(b"/t/1/k").is_empty());
}

/// `keys_of` iterates keys in `BTree` order however they were granted
/// (C-SIM replays seeds deterministically).
#[test]
fn row_table_keys_of_is_deterministic() {
    let t = RowLockTable::new();
    for key in [b"/t/1/k9", b"/t/1/k1", b"/t/1/k5", b"/t/1/k0"] {
        ok(t.grant(key, id(3), RowLockMode::KeyShare, 1, None));
    }
    let keys: Vec<Vec<u8>> = t.keys_of(id(3), 0).into_iter().map(|(k, _)| k).collect();
    assert_eq!(
        keys,
        vec![
            b"/t/1/k0".to_vec(),
            b"/t/1/k1".to_vec(),
            b"/t/1/k5".to_vec(),
            b"/t/1/k9".to_vec()
        ]
    );
}

/// The const relation matrix equals the spec's table, spelled out here as
/// its own literal, and is symmetric (§6).
#[test]
fn relation_matrix_matches_the_spec_literal() {
    use RelLockMode::*;
    #[rustfmt::skip]
    let spec = [
        // held:                    AS     RS     RE     SUE    S      SRE    E      AE
        /* requested AccessShare */ [false, false, false, false, false, false, false, true ],
        /* RowShare              */ [false, false, false, false, false, false, true , true ],
        /* RowExclusive          */ [false, false, false, false, true , true , true , true ],
        /* ShareUpdateExclusive  */ [false, false, false, true , true , true , true , true ],
        /* Share                 */ [false, false, true , true , false, true , true , true ],
        /* ShareRowExclusive     */ [false, false, true , true , true , true , true , true ],
        /* Exclusive             */ [false, true , true , true , true , true , true , true ],
        /* AccessExclusive       */ [true , true , true , true , true , true , true , true ],
    ];
    assert_eq!(REL_LOCK_CONFLICTS, spec);
    for a in [
        AccessShare,
        RowShare,
        RowExclusive,
        ShareUpdateExclusive,
        Share,
        ShareRowExclusive,
        Exclusive,
        AccessExclusive,
    ] {
        for b in [
            AccessShare,
            RowShare,
            RowExclusive,
            ShareUpdateExclusive,
            Share,
            ShareRowExclusive,
            Exclusive,
            AccessExclusive,
        ] {
            assert_eq!(
                a.conflicts_with(b),
                spec[a as usize][b as usize],
                "{a:?} vs held {b:?}"
            );
            assert_eq!(
                a.conflicts_with(b),
                b.conflicts_with(a),
                "the matrix is symmetric: {a:?} vs {b:?}"
            );
        }
    }
    // Spot checks the §6 text is readable off the matrix.
    assert!(!AccessShare.conflicts_with(Exclusive));
    assert!(AccessShare.conflicts_with(AccessExclusive));
    assert!(!Share.conflicts_with(Share));
    assert!(Share.conflicts_with(RowExclusive));
    assert!(RowShare.conflicts_with(Exclusive));
    assert!(!RowShare.conflicts_with(RowExclusive));
}
