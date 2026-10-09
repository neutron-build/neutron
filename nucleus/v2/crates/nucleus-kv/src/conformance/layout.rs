//! C-T0 §2.2 key and version-value layout for ts-in-key backends, exactly as
//! the spec defines it. Test helper for the reference GC filter (`SpecGcFilter`)
//! and the LSM resurrection model; not used by the kv itself.
//!
//! `L` must be prefix-free (C-Q3s P-PREFIX: no other logical key starts with
//! it). Tests use arbitrary byte strings as logical keys, made prefix-free by
//! a big-endian `u32` length prefix.
//!
//! ```text
//! intent        L ‖ 0x00
//! version @ts   L ‖ 0x01 ‖ be64(u64::MAX - ts) ‖ 0x01   # newest first
//! end(L)        L ‖ 0x02                            # exclusive upper bound
//! ```
//!
//! `[intent_key(L), end_key(L))` holds exactly the intent and versions of `L`;
//! the GC range for tombstone `L@t` is `[version_key(L, t), end_key(L))`.

use crate::{Key, Value};

/// `L` in the layout above: `be32(len) ‖ bytes`, prefix-free for any input.
pub fn logical(l: &[u8]) -> Key {
    let mut out = Vec::with_capacity(4 + l.len());
    let n = u32::try_from(l.len()).unwrap_or(u32::MAX);
    out.extend_from_slice(&n.to_be_bytes());
    out.extend_from_slice(l);
    out
}

/// The `k@INTENT` slot: sorts before every version of `L`.
pub fn intent_key(l: &[u8]) -> Key {
    let mut k = logical(l);
    k.push(0x00);
    k
}

/// The `k@ts` version key: versions of one logical key sort newest first.
pub fn version_key(l: &[u8], ts: u64) -> Key {
    let mut k = logical(l);
    k.push(0x01);
    k.extend_from_slice(&(u64::MAX - ts).to_be_bytes());
    k.push(0x01);
    k
}

/// Exclusive upper bound of every entry (intent and versions) of `L`.
pub fn end_key(l: &[u8]) -> Key {
    let mut k = logical(l);
    k.push(0x02);
    k
}

/// What `parse` found after the logical key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Entry {
    Intent,
    Version(u64),
}

/// Splits a key from [`intent_key`] or [`version_key`] back into `(L, entry)`.
/// `None` for anything else: `end_key`, a bad prefix, trailing bytes.
pub fn parse(key: &[u8]) -> Option<(Key, Entry)> {
    if key.len() < 5 {
        return None;
    }
    let mut n = [0u8; 4];
    n.copy_from_slice(&key[..4]);
    let len = u32::from_be_bytes(n) as usize;
    let split = 4usize.checked_add(len)?;
    if key.len() <= split {
        return None;
    }
    let l = key[4..split].to_vec();
    match key[split] {
        0x00 if key.len() == split + 1 => Some((l, Entry::Intent)),
        0x01 if key.len() == split + 10 && key[split + 9] == 0x01 => {
            let mut b = [0u8; 8];
            b.copy_from_slice(&key[split + 1..split + 9]);
            Some((l, Entry::Version(u64::MAX - u64::from_be_bytes(b))))
        }
        _ => None,
    }
}

/// Version value headers (C-T0 §2.2): `header ‖ payload`.
pub const HEADER_LIVE: u8 = 0x00;
pub const HEADER_LIVE_KEY_CHANGED: u8 = 0x01;
pub const HEADER_TOMBSTONE: u8 = 0x02;
pub const HEADER_MOVED_TOMBSTONE: u8 = 0x03;

/// A live version with `key_changed = false`.
pub fn live_value(payload: &[u8]) -> Value {
    let mut v = Vec::with_capacity(1 + payload.len());
    v.push(HEADER_LIVE);
    v.extend_from_slice(payload);
    v
}

/// A live version with `key_changed = true`.
pub fn key_changed_value(payload: &[u8]) -> Value {
    let mut v = Vec::with_capacity(1 + payload.len());
    v.push(HEADER_LIVE_KEY_CHANGED);
    v.extend_from_slice(payload);
    v
}

/// A tombstone: no payload.
pub fn tombstone_value() -> Value {
    vec![HEADER_TOMBSTONE]
}

/// A moved-tombstone (§5.1): no payload.
pub fn moved_tombstone_value() -> Value {
    vec![HEADER_MOVED_TOMBSTONE]
}

/// The §2.2 header of a version value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueKind {
    Live,
    LiveKeyChanged,
    Tombstone,
    MovedTombstone,
}

/// Classifies a version value by its §2.2 header. Tombstones have no payload;
/// live values carry one.
pub fn value_kind(value: &[u8]) -> Option<ValueKind> {
    match value.first() {
        Some(&HEADER_LIVE) => Some(ValueKind::Live),
        Some(&HEADER_LIVE_KEY_CHANGED) => Some(ValueKind::LiveKeyChanged),
        Some(&HEADER_TOMBSTONE) => Some(ValueKind::Tombstone),
        Some(&HEADER_MOVED_TOMBSTONE) => Some(ValueKind::MovedTombstone),
        _ => None,
    }
}

/// A tombstone or moved-tombstone: reads as not-found (C-T0 §4 step 2).
pub fn is_tombstone(value: &[u8]) -> bool {
    matches!(
        value_kind(value),
        Some(ValueKind::Tombstone | ValueKind::MovedTombstone)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn versions_of(l: &[u8]) -> Vec<Key> {
        vec![
            version_key(l, 0),
            version_key(l, 1),
            version_key(l, 90),
            version_key(l, 101),
            version_key(l, u64::MAX / 2),
            version_key(l, u64::MAX - 1),
        ]
    }

    #[test]
    fn intent_sorts_first_end_sorts_last() {
        for l in [&b"k"[..], b"", b"\x00\xff", b"row/7"] {
            let intent = intent_key(l);
            let end = end_key(l);
            for v in versions_of(l) {
                assert!(intent < v, "{l:?}: {intent:?} !< {v:?}");
                assert!(v < end, "{l:?}: {v:?} !< {end:?}");
            }
        }
    }

    #[test]
    fn versions_sort_newest_first() {
        let order: Vec<u64> = vec![u64::MAX - 1, 101, 100, 90, 3, 2, 1, 0];
        let keys: Vec<Key> = order.iter().map(|&ts| version_key(b"k", ts)).collect();
        for w in keys.windows(2) {
            assert!(w[0] < w[1], "{:?} !< {:?}", w[0], w[1]);
        }
    }

    #[test]
    fn distinct_logical_keys_never_interleave() {
        // Any byte strings as logical keys: the length prefix keeps them
        // prefix-free, so no entry of one falls inside another's range.
        for (a, b) in [
            (&b"a"[..], &b"ab"[..]),
            (b"", b"\x00"),
            (b"\x00\xff", b"\x01"),
            (b"row/7", b"row/9"),
        ] {
            let (ka, kb) = (logical(a), logical(b));
            let (first, second) = if ka < kb { (a, b) } else { (b, a) };
            assert!(ka != kb, "{a:?} and {b:?} collide");
            assert!(
                end_key(first) <= intent_key(second),
                "{first:?} entries overlap {second:?}"
            );
            for va in versions_of(first) {
                for vb in versions_of(second) {
                    assert!(va < vb, "{va:?} !< {vb:?} for {first:?} < {second:?}");
                }
            }
        }
    }

    #[test]
    fn parse_round_trips_intent_and_versions() {
        let l = b"row/7";
        let (out, e) = parse(&intent_key(l)).unwrap_or((Vec::new(), Entry::Intent));
        assert_eq!((out.as_slice(), e), (l.as_slice(), Entry::Intent));
        for ts in [0, 1, 90, 101, u64::MAX - 1, u64::MAX] {
            let (out, e) = parse(&version_key(l, ts)).unwrap_or((Vec::new(), Entry::Intent));
            assert_eq!(
                (out.as_slice(), e),
                (l.as_slice(), Entry::Version(ts)),
                "ts {ts}"
            );
        }
    }

    #[test]
    fn parse_rejects_end_keys_and_foreign_keys() {
        assert_eq!(parse(&end_key(b"k")), None);
        assert_eq!(parse(b""), None);
        assert_eq!(parse(b"short"), None);
        assert_eq!(parse(b"\x00\x00\x00\x05only"), None);
        // A length that runs past the key, and trailing bytes after a tag.
        assert_eq!(parse(b"\x00\x00\x00\x09abc\x00"), None);
        let mut v = version_key(b"k", 5);
        v.push(0);
        assert_eq!(parse(&v), None);
        let mut i = intent_key(b"k");
        i.extend_from_slice(&0u64.to_be_bytes());
        assert_eq!(parse(&i), None);
    }

    #[test]
    fn version_range_covers_exactly_the_versions() {
        // Intent + every version of `k`, mixed with another key's entries:
        // scanning [intent_key(k), end_key(k)) must return only k's entries,
        // newest first, and the GC range [version_key(k, 90), end_key(k))
        // must return k@90 and everything older.
        let mut map = std::collections::BTreeMap::new();
        map.insert(intent_key(b"j"), vec![9u8]);
        for ts in [10u64, 20, 90, 101] {
            map.insert(version_key(b"k", ts), ts.to_be_bytes().to_vec());
        }
        map.insert(intent_key(b"k"), vec![1u8]);
        let mut after_end = end_key(b"k");
        after_end.push(b'x');
        map.insert(after_end, vec![2u8]);
        let keys: Vec<Key> = map
            .range(intent_key(b"k")..end_key(b"k"))
            .map(|(k, _)| k.clone())
            .collect();
        assert_eq!(
            keys,
            vec![
                intent_key(b"k"),
                version_key(b"k", 101),
                version_key(b"k", 90),
                version_key(b"k", 20),
                version_key(b"k", 10),
            ]
        );
        let gc: Vec<Key> = map
            .range(version_key(b"k", 90)..end_key(b"k"))
            .map(|(k, _)| k.clone())
            .collect();
        assert_eq!(
            gc,
            vec![
                version_key(b"k", 90),
                version_key(b"k", 20),
                version_key(b"k", 10)
            ]
        );
    }

    #[test]
    fn value_headers() {
        assert_eq!(value_kind(&live_value(b"row")), Some(ValueKind::Live));
        assert_eq!(
            value_kind(&key_changed_value(b"row")),
            Some(ValueKind::LiveKeyChanged)
        );
        assert_eq!(value_kind(&tombstone_value()), Some(ValueKind::Tombstone));
        assert_eq!(
            value_kind(&moved_tombstone_value()),
            Some(ValueKind::MovedTombstone)
        );
        assert_eq!(value_kind(&[]), None);
        assert_eq!(value_kind(&[0x04]), None);
        assert!(is_tombstone(&tombstone_value()));
        assert!(is_tombstone(&moved_tombstone_value()));
        assert!(!is_tombstone(&live_value(b"")));
        assert!(!is_tombstone(&key_changed_value(b"")));
        assert_eq!(live_value(b"abc"), vec![0x00, b'a', b'b', b'c']);
        assert_eq!(key_changed_value(b""), vec![0x01]);
        assert_eq!(tombstone_value(), vec![0x02]);
        assert_eq!(moved_tombstone_value(), vec![0x03]);
    }
}
