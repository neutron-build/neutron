//! C-T1a tests: §2.2/§2.3 encoding — byte-equality with the reference layout
//! in the kv conformance suite (on keys made prefix-free by `layout::logical`),
//! ordering, round-trips, the right-split key rules, and that decoders never
//! panic on arbitrary bytes.

use nucleus_codec::{Collation, KeyColumn, KeyType, Value as CodecValue};
use nucleus_kv::conformance::layout;
use nucleus_kv::Key;
use nucleus_txn::encoding::{
    decode_intent, decode_version, encode_intent, encode_version, end_key, intent_key, parse_key,
    parse_sys_txn_key, sys_epoch_key, sys_gc_w_key, sys_ts_clock_key, sys_ts_hwm_key, sys_txn_key,
    sys_txn_prefix, sys_txn_prefix_end, version_key, Entry, VersionValue,
};

use nucleus_txn::{Intent, Layer, LayerData, RowLockMode, Ts, TxnId};

fn ok<T, E: std::fmt::Debug>(r: Result<T, E>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => panic!("unexpected error: {e:?}"),
    }
}

/// Deterministic xorshift* PRNG: no quickcheck dependency, stable seeds.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 33
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
    fn bytes(&mut self, max_len: usize) -> Vec<u8> {
        (0..self.below(max_len + 1))
            .map(|_| self.next() as u8)
            .collect()
    }
}

fn layer(seq: u32, data: LayerData, lock: RowLockMode) -> Layer {
    Layer {
        seq,
        data_seq: seq,
        data,
        lock,
    }
}

fn write(value: &[u8], key_changed: bool) -> LayerData {
    LayerData::Write {
        value: value.to_vec(),
        key_changed,
    }
}

fn sample_intent() -> Intent {
    Intent {
        txn: TxnId { epoch: 7, n: 9 },
        layers: vec![
            layer(1, write(b"row", false), RowLockMode::NoKeyUpdate),
            Layer {
                seq: 3,
                data_seq: 1, // lock-only change copies data_seq
                data: write(b"row", false),
                lock: RowLockMode::Update,
            },
            layer(4, LayerData::Delete { moved: true }, RowLockMode::Update),
            layer(6, LayerData::Absent, RowLockMode::Update),
        ],
    }
}

const LOGICAL: [&[u8]; 7] = [
    b"",
    b"k",
    b"/t/1/pk",
    b"/u/2/ab\x00c",
    b"/i/3/ab\x00pk",
    b"\x00\xff\x00",
    b"row/7",
];

#[test]
fn keys_are_byte_identical_to_conformance_layout() {
    // `layout` prefixes its logical keys with `be32(len)` to make test inputs
    // prefix-free; on such keys the two key builders must agree byte for
    // byte. The txn layer uses the caller's `L` as-is (draft 7.2 §2.2).
    for l in LOGICAL {
        let pf = layout::logical(l);
        assert_eq!(intent_key(&pf), layout::intent_key(l), "intent {l:?}");
        assert_eq!(end_key(&pf), layout::end_key(l), "end {l:?}");
        for ts in [0u64, 1, 5, 90, 101, u64::MAX - 1, u64::MAX] {
            assert_eq!(
                version_key(&pf, Ts(ts)),
                layout::version_key(l, ts),
                "version {l:?}@{ts}"
            );
        }
    }
    // On the stored key of a codec-encoded (already prefix-free) logical key,
    // `L` is the key as-is: no prefix, no transformation.
    let mut l = Vec::new();
    nucleus_codec::encode_key(
        &[KeyColumn::asc(KeyType::Text(Collation::C))],
        &[Some(CodecValue::Text("row/7".into()))],
        &mut l,
    )
    .unwrap_or_else(|e| panic!("{e:?}"));
    assert_eq!(intent_key(&l), [l.as_slice(), &[0x00]].concat());
    let mut v = l.clone();
    v.push(0x01);
    v.extend_from_slice(&(u64::MAX - 42u64).to_be_bytes());
    v.push(0x01);
    assert_eq!(version_key(&l, Ts(42)), v);
    assert_eq!(end_key(&l), [l.as_slice(), &[0x02]].concat());
}

#[test]
fn parse_agrees_with_conformance_layout() {
    fn conv(mine: Option<(&[u8], Entry)>) -> Option<(Vec<u8>, Option<u64>)> {
        mine.map(|(l, e)| {
            (
                l.to_vec(),
                match e {
                    Entry::Intent => None,
                    Entry::Version(ts) => Some(ts.0),
                },
            )
        })
    }
    fn conv_ref(theirs: Option<(Key, layout::Entry)>) -> Option<(Vec<u8>, Option<u64>)> {
        // `layout::parse` returns the caller's logical key; `parse_key` the
        // stored `L`. On layout keys, `L = layout::logical(l)`, so re-prefix
        // theirs before comparing.
        theirs.map(|(l, e)| {
            (
                layout::logical(&l),
                match e {
                    layout::Entry::Intent => None,
                    layout::Entry::Version(ts) => Some(ts),
                },
            )
        })
    }
    // The parsers agree on the layout's own keys (intent, versions, end key).
    for l in LOGICAL {
        for key in [
            layout::intent_key(l),
            layout::version_key(l, 0),
            layout::version_key(l, 42),
            layout::version_key(l, u64::MAX),
            layout::end_key(l),
        ] {
            assert_eq!(
                conv(parse_key(&key)),
                conv_ref(layout::parse(&key)),
                "{key:?}"
            );
        }
    }
    // Both reject keys that are neither (an end key is neither): here, one
    // that is too short to be a version and does not end in a tag byte.
    for key in [&b""[..], b"short", b"\x00\x00\x00\x09abc\xff"] {
        assert_eq!(conv(parse_key(key)), None, "{key:?}");
    }
}

#[test]
fn parse_splits_from_the_right_without_a_schema() {
    // §2.2 draft 7.2: the last byte decides. Round-trips hold for arbitrary
    // logical keys (prefix-free or not) because the split is from the right.
    let mut rng = Lcg(0x9e3779b97f4a7c15);
    for _ in 0..2000 {
        let l = rng.bytes(24);
        let got = parse_key(&intent_key(&l)).map(|(o, e)| (o.to_vec(), e));
        assert_eq!(got, Some((l.clone(), Entry::Intent)), "intent {l:?}");
        let ts = Ts(rng.next());
        let got = parse_key(&version_key(&l, ts)).map(|(o, e)| (o.to_vec(), e));
        assert_eq!(got, Some((l.clone(), Entry::Version(ts))), "version {l:?}");
        assert_eq!(parse_key(&end_key(&l)), None, "end {l:?}");
    }
    // The empty logical key.
    assert_eq!(parse_key(&[0x00]), Some((&[][..], Entry::Intent)));
    let mut v = vec![0x01];
    v.extend_from_slice(&(u64::MAX - 7u64).to_be_bytes());
    v.push(0x01);
    assert_eq!(parse_key(&v), Some((&[][..], Entry::Version(Ts(7)))));
    // A logical key that itself ends in the tag bytes still round-trips.
    for l in [&b"\x00"[..], b"\x01", b"a\x00", b"a\x01", b"\x00\x01\x00"] {
        assert_eq!(
            parse_key(&intent_key(l)).map(|(o, e)| (o.to_vec(), e)),
            Some((l.to_vec(), Entry::Intent)),
            "{l:?}"
        );
    }
    // Version rule: the byte 9 before the final 0x01 must be the 0x01
    // separator; anything else is None (here: 0x00, 0x02, or a too-short key).
    let mut bad = vec![0x00u8];
    bad.extend_from_slice(&[0u8; 8]);
    bad.push(0x01);
    assert_eq!(parse_key(&bad), None, "separator not 0x01");
    let mut short = vec![0x01, 0x01];
    short.extend_from_slice(&[0u8; 5]);
    short.push(0x01);
    assert_eq!(parse_key(&short), None, "shorter than 10 bytes");
    // A key ending in a foreign byte is never ours.
    assert_eq!(parse_key(b"/sys/epoch"), None);
    assert_eq!(parse_key(b"/sys/gc_w"), None);
}

#[test]
fn ordering_intent_first_versions_newest_first_end_bounds() {
    for l in LOGICAL {
        let intent = intent_key(l);
        let end = end_key(l);
        let mut prev: Option<Key> = None;
        // Newest first: u64::MAX down to 0 must sort ascending in stored form.
        for ts in [u64::MAX, 101, 100, 90, 3, 2, 1, 0] {
            let k = version_key(l, Ts(ts));
            assert!(intent < k, "{l:?}: {intent:?} !< {k:?}");
            assert!(k < end, "{l:?}: {k:?} !< {end:?}");
            if let Some(p) = prev {
                assert!(p < k, "{l:?}: {p:?} !< {k:?}");
            }
            prev = Some(k);
        }
    }
}

#[test]
fn version_values_round_trip_and_headers() {
    let cases = [
        write(b"", false),
        write(b"", true),
        write(b"row", false),
        write(b"row", true),
        LayerData::Delete { moved: false },
        LayerData::Delete { moved: true },
    ];
    for data in &cases {
        let value = encode_version(data).unwrap_or_else(|| panic!("no version for {data:?}"));
        let decoded = ok(decode_version(&value));
        let want = match data {
            LayerData::Write { value, key_changed } => VersionValue::Live {
                payload: value.clone(),
                key_changed: *key_changed,
            },
            LayerData::Delete { moved } => VersionValue::Tombstone { moved: *moved },
            LayerData::Absent => unreachable!(),
        };
        assert_eq!(decoded, want);
        assert_eq!(
            decoded.into_option().is_none(),
            matches!(data, LayerData::Delete { .. })
        );
    }
    // Absent produces no version (§7.3 step 3).
    assert!(encode_version(&LayerData::Absent).is_none());
    // Exact bytes (§2.2 headers).
    assert_eq!(
        encode_version(&write(b"abc", false)),
        Some(vec![0x00, b'a', b'b', b'c'])
    );
    assert_eq!(encode_version(&write(b"", true)), Some(vec![0x01]));
    assert_eq!(
        encode_version(&LayerData::Delete { moved: false }),
        Some(vec![0x02])
    );
    assert_eq!(
        encode_version(&LayerData::Delete { moved: true }),
        Some(vec![0x03])
    );
    // Corrupt input is an error, never a panic. ([0x00] alone is a legal
    // empty live value; [0x01] likewise.)
    for bad in [
        &[][..],
        &[0x04][..],
        &[0xff][..],
        &[0x02, 0x00][..],
        &[0x03, 0xff][..],
    ] {
        assert!(decode_version(bad).is_err(), "{bad:?}");
    }
}

#[test]
fn intent_round_trip() {
    let cases = [
        Intent {
            txn: TxnId { epoch: 0, n: 0 },
            layers: vec![layer(1, LayerData::Absent, RowLockMode::KeyShare)],
        },
        sample_intent(),
        Intent {
            txn: TxnId {
                epoch: u32::MAX,
                n: u64::MAX,
            },
            layers: vec![layer(
                1,
                write(&[0x00, 0xff, 0x00], false),
                RowLockMode::Share,
            )],
        },
    ];
    for intent in &cases {
        let value = ok(encode_intent(intent));
        assert_eq!(&value[..4], b"NTXI");
        assert_eq!(value[4], 1);
        assert_eq!(ok(decode_intent(&value)), *intent);
    }
    // Every lock mode round-trips.
    for (i, lock) in [
        RowLockMode::KeyShare,
        RowLockMode::Share,
        RowLockMode::NoKeyUpdate,
        RowLockMode::Update,
    ]
    .into_iter()
    .enumerate()
    {
        let intent = Intent {
            txn: TxnId { epoch: 1, n: 1 },
            layers: vec![layer(1 + i as u32, LayerData::Absent, lock)],
        };
        assert_eq!(ok(decode_intent(&ok(encode_intent(&intent)))), intent);
    }
    // §2.1: layers are never empty.
    assert!(encode_intent(&Intent {
        txn: TxnId { epoch: 1, n: 1 },
        layers: Vec::new(),
    })
    .is_err());
    assert!(decode_intent(b"NTXI\x01").is_err());
}

#[test]
fn decode_intent_rejects_structural_corruption() {
    let valid = ok(encode_intent(&sample_intent()));
    // Truncated at every length: error or (never) a valid prefix-free decode.
    for n in 0..valid.len() {
        let r = decode_intent(&valid[..n]);
        assert!(r.is_err(), "truncation at {n} decoded to {r:?}");
    }
    // Trailing garbage.
    let mut trailing = valid.clone();
    trailing.push(0x00);
    assert!(decode_intent(&trailing).is_err());
    // Non-monotonic layer seqs cannot be produced, so hand-build one.
    let bad = Intent {
        txn: TxnId { epoch: 1, n: 1 },
        layers: vec![
            layer(5, LayerData::Absent, RowLockMode::Update),
            layer(3, LayerData::Absent, RowLockMode::Update),
        ],
    };
    assert!(decode_intent(&ok(encode_intent(&bad))).is_err());
    // Unknown format version.
    let mut future = valid.clone();
    future[4] = 0x02;
    assert!(decode_intent(&future).is_err());
}

#[test]
fn decode_intent_never_panics_on_arbitrary_bytes() {
    let mut rng = Lcg(0x853c49e6748fea9b);
    let valid = ok(encode_intent(&sample_intent()));
    let mut decoded_ok = 0usize;

    // Mutations of a valid encoding: flips, truncations, extensions.
    for _ in 0..20_000 {
        let mut b = valid.clone();
        match rng.below(3) {
            0 => {
                let i = rng.below(b.len());
                b[i] = rng.next() as u8;
            }
            1 => {
                b.truncate(rng.below(b.len() + 1));
            }
            _ => {
                b.extend_from_slice(&rng.bytes(8));
            }
        }
        if decode_intent(&b).is_ok() {
            decoded_ok += 1;
        }
    }
    // Fully random buffers.
    for _ in 0..50_000 {
        let b = rng.bytes(80);
        if decode_intent(&b).is_ok() {
            decoded_ok += 1;
        }
    }
    // Every 0-, 1- and 2-byte buffer over the first magic byte.
    for a in [0u8, 0x01, 0xff, b'N'] {
        let _ = decode_intent(&[a]);
        for b2 in [0u8, 0x01, 0xff, b'T'] {
            let _ = decode_intent(&[a, b2]);
        }
    }
    // The unmutated encoding still decodes: the fuzz is not vacuously all-Err.
    assert!(decode_intent(&valid).is_ok());
    assert!(decoded_ok > 0, "every mutated input failed to decode");
}

#[test]
fn system_keys_sort_outside_data_prefixes() {
    let data = [
        &b"/t/1/pk"[..],
        b"/t/1",
        b"/u/2/ab\x00c",
        b"/u/2/ab\x00c/longer",
        b"/i/3/ab\x00pk",
        b"/i/3/ab",
        b"/sys/epoch", // a logical key that literally collides with a sys key name
    ];
    let sys = [
        sys_txn_key(TxnId { epoch: 0, n: 0 }),
        sys_txn_key(TxnId {
            epoch: u32::MAX,
            n: u64::MAX,
        }),
        sys_epoch_key(),
        sys_ts_hwm_key(),
        sys_gc_w_key(),
        sys_ts_clock_key(Ts(0)),
        sys_ts_clock_key(Ts(u64::MAX)),
    ];
    for d in data {
        let lo = intent_key(d);
        let hi = end_key(d);
        for s in &sys {
            assert!(
                s < &lo || s >= &hi,
                "sys key {s:?} falls inside the entries of {d:?}"
            );
        }
    }
    // The txn-record range covers exactly the /sys/txn/ records and no other
    // system key.
    let (lo, hi) = (sys_txn_prefix(), sys_txn_prefix_end());
    for s in &sys {
        let is_record = matches!(
            s.strip_prefix(b"/sys/txn/".as_slice()),
            Some(rest) if rest.len() == 12
        );
        assert_eq!(s >= &lo && s < &hi, is_record, "{s:?}");
    }
    // Ids round-trip through the key.
    for id in [
        TxnId { epoch: 0, n: 0 },
        TxnId { epoch: 1, n: 1 },
        TxnId {
            epoch: u32::MAX,
            n: u64::MAX,
        },
    ] {
        assert_eq!(parse_sys_txn_key(&sys_txn_key(id)), Some(id));
    }
    assert_eq!(parse_sys_txn_key(b"/sys/txn/"), None);
    assert_eq!(parse_sys_txn_key(b"/sys/txn/\x00\x00\x00\x01"), None);
    assert_eq!(parse_sys_txn_key(&sys_epoch_key()), None);
    // All distinct.
    let mut sorted = sys.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(sorted.len(), sys.len());
}
