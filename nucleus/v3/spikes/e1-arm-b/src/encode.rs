//! Key and value encoding of arm (b): timestamp-in-key versions
//! (card X-2 Work 2; PLAN-v3 §4 E1′, §2 N3 option (b)).
//!
//! Every version of a user key is one B+tree entry. The entry key is
//! `escape(user_key) ++ [0x00, 0x00] ++ !ts.to_be_bytes()`, so the
//! tree's memcmp order over entries is **(user key asc, ts desc)**:
//!
//! - `0x00` bytes of the user key are escaped to `0x00 0xFF`, so an
//!   escaped key can never contain `0x00 0x00`; the `0x00 0x00`
//!   terminator is therefore unambiguous, and escaping is monotone —
//!   for user keys `u1 < u2` every encoded entry of `u1` sorts below
//!   the whole group of `u2` (proof sketch: at the first differing
//!   byte the escape emits `0x00` only for a `0x00` input, which is
//!   the smallest possible output; when `u1` is a proper prefix of
//!   `u2`, the terminator's second `0x00` wins against the follower
//!   `0xFF` of any escaped `0x00`);
//! - the terminator ends the key part, so a version of `u` can never
//!   sort as a version of a longer key that extends `u`;
//! - `!ts` (bitwise-not, big-endian) inverts the ts order, so within
//!   one user key the **newest** version sorts first — a read at
//!   snapshot `S` seeks `enc_prefix ++ !S` and the first hit is the
//!   newest version with `ts <= S` (card Work 3).
//!
//! Invariants owned here:
//! - B-ENC-ORDER: for any set of `(user key, ts)` pairs, sorting the
//!   encoded keys equals sorting the pairs by `(key asc, ts desc)`
//!   (pinned by the proptest below; card Work 2). Measured catchers of
//!   the card's `TS-ASC` mutant (ts encoded ascending, decode kept
//!   consistent so only the order flips): S13 — 994 failing assertions
//!   across the 500 seeds — plus S1/S4/S5/S6/S8–S12 collaterally. S2
//!   alone does **not** catch it: its RR reader registers its snapshot
//!   immediately after the seed commit, so the snapshot ts equals the
//!   only older version's ts and "oldest version >= S" and
//!   "newest version <= S" coincide on that schedule; the mutant only
//!   diverges when some version's ts falls strictly between the
//!   reader's snapshot and a newer version, which the seeded
//!   differential produces constantly and S2's fixed schedule never.
//! - B-ENC-ROUNDTRIP: `decode_key(encode_key(k, ts)) == Some((k, ts))`
//!   for every user key and ts, and `decode_value(encode_value(v)) ==
//!   Some(v)` for every value — a decode failure is corruption, never
//!   a representation limit of data this arm wrote.
//! - B-TOMBSTONE: a tombstone is value tag `1` and decodes to `None`;
//!   a value is tag `0` followed by its bytes (card Work 2). Reading a
//!   tombstone as an empty value is the TOMBSTONE-AS-VALUE mutant,
//!   caught by S8.

use e1_mvcc::api::Ts;

/// Value tag: the entry holds a value (card Work 2).
pub(crate) const TAG_VALUE: u8 = 0;

/// Value tag: the entry is a tombstone (card Work 2).
pub(crate) const TAG_TOMBSTONE: u8 = 1;

/// The escaped `0x00` byte of a user key (see the module docs).
pub(crate) const ESCAPED_ZERO: [u8; 2] = [0x00, 0xFF];

/// The terminator between the escaped user key and the ts (card Work 2).
pub(crate) const TERMINATOR: [u8; 2] = [0x00, 0x00];

/// Length of the appended `!ts` (a big-endian `u64`).
pub(crate) const TS_BYTES: usize = 8;

/// `escape(key) ++ TERMINATOR`: the encoded prefix whose group holds
/// every version of `key` (card Work 2). All entries of `key` start
/// with these bytes and no other key's entries do.
pub(crate) fn prefix_of(key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(2 * key.len() + TERMINATOR.len());
    for &b in key {
        if b == 0x00 {
            out.extend_from_slice(&ESCAPED_ZERO);
        } else {
            out.push(b);
        }
    }
    out.extend_from_slice(&TERMINATOR);
    out
}

/// The full entry key of one version: `prefix_of(key) ++ !ts`
/// (card Work 2). Also the seek key of a snapshot-`ts` read at `ts`
/// (card Work 3): entries at or after it are exactly the versions with
/// `ts' <= ts`.
pub(crate) fn encode_key(key: &[u8], ts: Ts) -> Vec<u8> {
    let mut out = prefix_of(key);
    out.extend_from_slice(&(!ts).to_be_bytes());
    out
}

/// The seek key of the newest-version probe (card Work 4):
/// `enc(k) ++ !MAX` — `!u64::MAX` is all zero bytes, the smallest ts
/// suffix possible, so the first entry at or after it carrying `key`'s
/// prefix is the newest version of `key`, whatever its ts.
pub(crate) fn encode_seek_newest(key: &[u8]) -> Vec<u8> {
    encode_key(key, Ts::MAX)
}

/// An exclusive upper bound covering every version of `key`: the prefix
/// plus one more `0xFF` than any ts suffix can hold, so the half-open
/// range `[seek, bound)` contains all of `key`'s entries and nothing
/// beyond them.
pub(crate) fn encode_key_upper(key: &[u8]) -> Vec<u8> {
    let mut out = prefix_of(key);
    out.extend(std::iter::repeat_n(0xFF, TS_BYTES + 1));
    out
}

/// An exclusive upper bound covering every entry the tree can hold
/// (encoded keys are capped at [`e0_btree::page::MAX_KEY`] bytes by the
/// tree itself): an all-`0xFF` key one byte longer sorts above all of
/// them. Used by the whole-tree passes of `gc` and `stats`.
pub(crate) fn encode_table_upper() -> Vec<u8> {
    vec![0xFF; e0_btree::page::MAX_KEY + 1]
}

/// Split an entry key back into its user key and ts. `None` means the
/// key is not one this module could have encoded (corruption; the
/// engine surfaces it as `Engine`, never a panic — B-ENC-ROUNDTRIP's
/// failure half).
pub(crate) fn decode_key(entry: &[u8]) -> Option<(Vec<u8>, Ts)> {
    let n = entry.len();
    if n < TERMINATOR.len() + TS_BYTES {
        return None;
    }
    let body = entry.get(..n - TS_BYTES)?;
    let mut ts_bytes = [0u8; TS_BYTES];
    for (i, slot) in ts_bytes.iter_mut().enumerate() {
        *slot = *entry.get(n - TS_BYTES + i)?;
    }
    let ts = !u64::from_be_bytes(ts_bytes);
    let m = body.len();
    if body.get(m - 2)? != &TERMINATOR[0] || body.get(m - 1)? != &TERMINATOR[1] {
        return None;
    }
    let escaped = body.get(..m - TERMINATOR.len())?;
    // Unescape: a `0x00` in escaped data is always the lead of the
    // `0x00 0xFF` pair, so a lone `0x00` (or a trailing one) is not
    // ours.
    let mut key = Vec::with_capacity(escaped.len());
    let mut i = 0usize;
    while i < escaped.len() {
        let b = *escaped.get(i)?;
        if b == 0x00 {
            if *escaped.get(i + 1)? != ESCAPED_ZERO[1] {
                return None;
            }
            key.push(0x00);
            i += 2;
        } else {
            key.push(b);
            i += 1;
        }
    }
    Some((key, ts))
}

/// The entry value of one version: tag `0` plus the bytes, or tag `1`
/// alone for a tombstone (card Work 2). One byte longer than the value.
pub(crate) fn encode_value(value: Option<&[u8]>) -> Vec<u8> {
    match value {
        Some(v) => {
            let mut out = Vec::with_capacity(1 + v.len());
            out.push(TAG_VALUE);
            out.extend_from_slice(v);
            out
        }
        None => vec![TAG_TOMBSTONE],
    }
}

/// Decode an entry value. `Some(None)` is a tombstone, `Some(Some(v))`
/// a value; `None` means the bytes are not one this module could have
/// encoded (corruption).
pub(crate) fn decode_value(entry: &[u8]) -> Option<Option<Vec<u8>>> {
    match entry.first() {
        Some(&TAG_VALUE) => Some(Some(entry.get(1..)?.to_vec())),
        Some(&TAG_TOMBSTONE) => Some(None),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        TAG_TOMBSTONE, TAG_VALUE, decode_key, decode_value, encode_key, encode_seek_newest,
        encode_value, prefix_of,
    };
    use proptest::prelude::*;

    /// B-ENC-ORDER (card X-2 Work 2): the encoded order equals the
    /// `(key asc, Reverse(ts))` order. Keys deliberately include `0x00`
    /// bytes (the escape's hard case: a `0x00` in one key against a
    /// `0x01` in the other, and prefix-of relations).
    #[test]
    fn proptest_encoded_order_is_key_asc_then_ts_desc() {
        proptest!(|(pairs in proptest::collection::vec(
            (proptest::collection::vec(any::<u8>(), 0..=12), 0u64..1000),
            1..=40,
        ))| {
            let mut model: Vec<(Vec<u8>, u64)> = pairs;
            model.sort();
            model.dedup();
            // The expected order: user key ascending, then ts descending.
            let mut want = model.clone();
            want.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));
            let mut got: Vec<Vec<u8>> = model.iter().map(|(k, ts)| encode_key(k, *ts)).collect();
            got.sort();
            let expect: Vec<Vec<u8>> = want.iter().map(|(k, ts)| encode_key(k, *ts)).collect();
            prop_assert!(
                got == expect,
                "encoded order != (key asc, ts desc): got {got:?}, want {expect:?}"
            );
            // B-ENC-ROUNDTRIP on the same corpus, in expected order.
            for (entry, (key, ts)) in expect.iter().zip(want.iter()) {
                prop_assert_eq!(decode_key(entry), Some((key.clone(), *ts)));
            }
        });
    }

    /// B-ENC-ORDER, directed cases around the escape's hard corners:
    /// `0x00` against `0x01` at the differing byte, and a key that is a
    /// proper prefix of another (the terminator must keep the shorter
    /// key's whole group below the longer key's).
    #[test]
    fn escape_orders_zeros_and_prefixes_correctly() {
        let cases: Vec<(Vec<u8>, Vec<u8>)> = vec![
            (vec![0x00], vec![0x01]),
            (vec![0x00, 0x00], vec![0x00, 0x01]),
            (vec![0x00, 0xFF], vec![0x01]),
            (vec![], vec![0x00]),
            (vec![0x61], vec![0x61, 0x00]),
            (vec![0x61, 0x00], vec![0x61, 0x00, 0x00]),
            (vec![0x61, 0x00], vec![0x61, 0x01]),
        ];
        for (small, big) in cases {
            assert!(
                small < big,
                "precondition: {small:?} < {big:?} as user keys"
            );
            // The whole group of `small` (any ts) sorts below the whole
            // group of `big` (any ts).
            for ts_small in [1u64, 7, u64::MAX] {
                for ts_big in [1u64, 7, u64::MAX] {
                    assert!(
                        encode_key(&small, ts_small) < encode_key(&big, ts_big),
                        "group of {small:?}@{ts_small} must sort below {big:?}@{ts_big}"
                    );
                }
            }
        }
    }

    /// B-ENC-ROUNDTRIP, directed: empty key, zero-heavy keys, both ts
    /// extremes, and the value tags.
    #[test]
    fn encode_decode_roundtrip() {
        for key in [
            vec![],
            vec![0x00],
            vec![0x00, 0x00, 0x01],
            vec![0xFF, 0x00, 0xFE],
            b"a plain key".to_vec(),
        ] {
            for ts in [0u64, 1, 42, u64::MAX - 1, u64::MAX] {
                let entry = encode_key(&key, ts);
                assert_eq!(decode_key(&entry), Some((key.clone(), ts)));
            }
            // The seek for the newest version is the all-zero ts suffix
            // (the smallest), inside the key's own group.
            assert!(encode_seek_newest(&key).starts_with(&prefix_of(&key)));
            assert_eq!(encode_seek_newest(&key), encode_key(&key, u64::MAX));
        }
        for value in [Some(vec![]), Some(b"value".to_vec()), None] {
            let enc = encode_value(value.as_deref());
            assert_eq!(decode_value(&enc), Some(value));
        }
        assert_eq!(encode_value(None), vec![TAG_TOMBSTONE]);
        assert_eq!(encode_value(Some(b"")), vec![TAG_VALUE]);
        // Not encodable: corruption, not a panic.
        assert_eq!(decode_key(b"no terminator"), None);
        assert_eq!(decode_key(&encode_key(b"k", 1)[..6]), None);
        assert_eq!(decode_value(&[]), None);
        assert_eq!(decode_value(&[7, 1]), None);
    }
}
