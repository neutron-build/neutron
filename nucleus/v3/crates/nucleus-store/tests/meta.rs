//! Card A-2-0's tests (Work 6): page 0's and the meta record's codecs with
//! golden vectors, the replica region, the boot-id order, the open rule's
//! table form `t_meta_copies` (S-STORE §2.2, §15.1) against an independent
//! oracle, and the fuzz targets for the page-0 and meta decoders and the
//! open rule, driven by proptest.
//!
//! The fixture is held still for every test in this file (card A-2-0, Work
//! 6): page size 4096, `wal_seg_size` 64 MiB, `format_version` 1,
//! `checksum_alg` 1, the own `creation_uuid` sixteen bytes of 0x11 and the
//! foreign one sixteen bytes of 0x22. The offsets this file uses (the CRCs
//! at 37 and 163, the `copy` byte at 12, the torn bytes 20 and 50) are its
//! own, from the card's tables, and not read from the crate, so the golden
//! vectors and the re-sealed cases pin the layout independently.

use std::collections::BTreeMap;

use nucleus_store::StoreError;
use nucleus_store::crc::crc32c;
use nucleus_store::meta::{
    CHECKSUM_CRC32C, DEFAULT_PAGE_SIZE, DEFAULT_WAL_SEG_SIZE, FORMAT_VERSION, Identity, META_LEN,
    META_MAGIC, Meta, MetaCopy, PAGE_SIZES, PAGE0_LEN, PAGE0_MAGIC, PAGE0_RECORD_LEN,
    REPLICA_BLOCK_LEN, REPLICA_OFFSETS, Ref, boot_id_older, decode_meta, decode_page0, encode_meta,
    encode_page0, is_page_size, meta_parity, meta_slot, min_file_slots, reserved_slots,
};
use nucleus_store::open::{
    Block, HeaderBlocks, OpenCorrupt, OpenDecision, locate_page_size, select_meta,
};
use proptest::prelude::*;
use proptest::sample::Index;

// ---------------------------------------------------------------------
// The fixture
// ---------------------------------------------------------------------

/// The file's own `creation_uuid`.
const OWN_UUID: [u8; 16] = [0x11; 16];
/// Another file's `creation_uuid`.
const FOREIGN_UUID: [u8; 16] = [0x22; 16];
/// Page 0's CRC field: after bytes 0..37 (§2.1, the card's table).
const PAGE0_CRC_AT: usize = 37;
/// The meta record's CRC field: after bytes 0..163 (§2.2, the card's table).
const META_CRC_AT: usize = 163;
/// The meta record's `copy` byte.
const COPY_AT: usize = 12;
/// The byte flipped in a torn copy (inside `T`).
const TORN_META_BYTE: usize = 50;
/// The byte flipped in a torn page 0 (inside `wal_seg_size`).
const TORN_PAGE0_BYTE: usize = 20;

/// The golden page-0 record of the fixture identity (card A-2-0 Work 6,
/// computed by the card's author with an independent bitwise CRC32C).
const PAGE0_GOLDEN: &str =
    "4e55434c4555530001000000001000000100000004111111111111111111111111111111114548abf4";

/// The golden meta record of `V(10)`, primary copy (card A-2-0 Work 6).
const META_GOLDEN: &str = "4e55434d455441000100000000001000000100000004111111111111111111111111111111110a00000000000000f203000000000000fe010000000000000a0000006e000000000000000a0000000a0000a001d2000000000000000a0000000a0000b007000000000000004a00000000000000001000000000000000000a0000000000cbff0900000000000a0000c0e8030a0000000000240400000000000005000000affb9a2f";

/// The fixture identity with `uuid`.
fn identity_with(uuid: [u8; 16]) -> Identity {
    Identity {
        format_version: 1,
        page_size: 4096,
        checksum_alg: 1,
        wal_seg_size: 67_108_864,
        creation_uuid: uuid,
    }
}

/// The file's own identity.
fn own() -> Identity {
    identity_with(OWN_UUID)
}

/// `base(c, boot, t_bump)` (card A-2-0 Work 6), with the own identity.
fn base(c: u64, boot: u32, t_bump: u64) -> Meta {
    Meta {
        identity: own(),
        ckpt_id: c,
        t: 1000 + c + t_bump,
        gc_w: 500 + c,
        epoch: c as u32,
        map_root: Ref {
            slot: 100 + c,
            epoch: c as u32,
            crc: 0xA000_0000 + c as u32,
        },
        map_level: 1,
        free_root: Ref {
            slot: 200 + c,
            epoch: c as u32,
            crc: 0xB000_0000 + c as u32,
        },
        dir_root: 7,
        next_lpid: 64 + c,
        slot_count: 4096,
        wal_start_lsn: 65536 * c,
        wal_start_prev: 65536 * c - 53,
        wal_start_prev_crc: 0xC000_0000 + c as u32,
        wal_ckpt_end: 65536 * c + 1000,
        ts_ckpt_hi: 1050 + c,
        boot_id: boot,
    }
}

/// `V(c)`: a valid copy's meta.
fn v(c: u64) -> Meta {
    base(c, 5, 0)
}

/// `Vn(c)`: a newer `boot_id` and another field.
fn vn(c: u64) -> Meta {
    base(c, 6, 1)
}

/// `E(c)`: the same `boot_id`, another field differing.
fn e(c: u64) -> Meta {
    base(c, 5, 2)
}

/// `base(c, 5, 0)` with the foreign identity.
fn foreign(c: u64) -> Meta {
    Meta {
        identity: identity_with(FOREIGN_UUID),
        ..base(c, 5, 0)
    }
}

/// Lower-case hex of `b`.
fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// The little-endian `u32` at `at`.
fn u32_at(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes([b[at], b[at + 1], b[at + 2], b[at + 3]])
}

/// Re-seals a record in place: the four bytes at `crc_at` become the CRC32C
/// of the bytes before them, when the record is long enough to hold them.
fn reseal(rec: &mut [u8], crc_at: usize) {
    if rec.len() >= crc_at + 4 {
        let crc = crc32c(0, &rec[..crc_at]);
        rec[crc_at..crc_at + 4].copy_from_slice(&crc.to_le_bytes());
    }
}

/// Whether `r` is an XX001 (`StoreError::Corrupt`).
fn is_corrupt<T>(r: &Result<T, StoreError>) -> bool {
    matches!(r, Err(StoreError::Corrupt(_)))
}

/// Whether `r` is a caller's misuse (`StoreError::Invalid`).
fn is_invalid<T>(r: &Result<T, StoreError>) -> bool {
    matches!(r, Err(StoreError::Invalid(_)))
}

// ---------------------------------------------------------------------
// The codecs
// ---------------------------------------------------------------------

#[test]
fn page0_codec() {
    assert_eq!(
        (PAGE0_MAGIC, PAGE0_LEN, PAGE0_RECORD_LEN),
        (*b"NUCLEUS\0", 4096, 41)
    );
    assert_eq!(
        (
            FORMAT_VERSION,
            CHECKSUM_CRC32C,
            DEFAULT_PAGE_SIZE,
            DEFAULT_WAL_SEG_SIZE
        ),
        (1, 1, 8192, 64 << 20)
    );
    let id = own();
    let block = encode_page0(&id);
    assert_eq!(decode_page0(&block).unwrap(), id);
    // The golden vector: the record's 41 bytes, crc 0xF4AB4845; zeros after.
    assert_eq!(hex(&block[..41]), PAGE0_GOLDEN);
    assert_eq!(u32_at(&block, PAGE0_CRC_AT), 0xF4AB_4845);
    assert!(block[41..].iter().all(|&b| b == 0));
    // The record alone decodes, and so does a block with any tail.
    assert_eq!(decode_page0(&block[..41]).unwrap(), id);
    let mut tailed = block.to_vec();
    tailed[41..].fill(0xA5);
    assert_eq!(decode_page0(&tailed).unwrap(), id);
    // Every allowed page size round-trips.
    for ps in PAGE_SIZES {
        let id = Identity {
            page_size: ps,
            ..own()
        };
        assert_eq!(
            decode_page0(&encode_page0(&id)).unwrap(),
            id,
            "page size {ps}"
        );
    }
    // A block shorter than the record.
    for len in 0..41 {
        assert!(is_corrupt(&decode_page0(&block[..len])), "{len} bytes");
    }
    // A wrong magic, with the crc left and re-sealed: the magic refuses it.
    let mut wrong = block;
    wrong[6] = b'X';
    assert!(is_corrupt(&decode_page0(&wrong)));
    reseal(&mut wrong, PAGE0_CRC_AT);
    assert!(is_corrupt(&decode_page0(&wrong)));
    // A meta record is not page 0.
    assert!(is_corrupt(&decode_page0(&encode_meta(
        &v(10),
        MetaCopy::Primary
    ))));
    // A flipped byte in each of the 41 positions.
    for i in 0..41 {
        let mut b = block;
        b[i] ^= 0xFF;
        assert!(is_corrupt(&decode_page0(&b)), "byte {i} flipped decodes");
    }
    // Out-of-domain fields with a correct crc: the domain check refuses
    // them.
    let bad = [
        Identity {
            format_version: 2,
            ..own()
        },
        Identity {
            page_size: 12288,
            ..own()
        },
        Identity {
            checksum_alg: 2,
            ..own()
        },
        Identity {
            wal_seg_size: 0,
            ..own()
        },
    ];
    for id in bad {
        let b = encode_page0(&id);
        assert_eq!(u32_at(&b, PAGE0_CRC_AT), crc32c(0, &b[..PAGE0_CRC_AT]));
        assert!(is_corrupt(&decode_page0(&b)), "{id:?} decodes");
    }
}

#[test]
fn meta_codec() {
    assert_eq!((META_MAGIC, META_LEN), (*b"NUCMETA\0", 167));
    let primary = encode_meta(&v(10), MetaCopy::Primary);
    assert_eq!(decode_meta(&primary).unwrap(), (v(10), MetaCopy::Primary));
    // The golden vector, crc 0x2F9AFBAF.
    assert_eq!(hex(&primary), META_GOLDEN);
    assert_eq!(u32_at(&primary, META_CRC_AT), 0x2F9A_FBAF);
    // The replica copy: crc 0x416E891A, and it differs from the primary in
    // the copy byte and the crc only.
    let replica = encode_meta(&v(10), MetaCopy::Replica);
    assert_eq!(decode_meta(&replica).unwrap(), (v(10), MetaCopy::Replica));
    assert_eq!(u32_at(&replica, META_CRC_AT), 0x416E_891A);
    let differ: Vec<usize> = (0..META_LEN)
        .filter(|&i| primary[i] != replica[i])
        .collect();
    assert_eq!(differ, [COPY_AT, 163, 164, 165, 166]);
    assert_eq!((primary[COPY_AT], replica[COPY_AT]), (0, 1));
    // Vn(10)'s primary copy.
    let newer = encode_meta(&vn(10), MetaCopy::Primary);
    assert_eq!(u32_at(&newer, META_CRC_AT), 0xD91A_73C5);
    assert_eq!(decode_meta(&newer).unwrap(), (vn(10), MetaCopy::Primary));
    // A 4096-byte block whose first 167 bytes are the record decodes the
    // same, whatever its tail holds.
    for tail in [0x00u8, 0xFF, 0x5A] {
        let mut b = vec![tail; 4096];
        b[..META_LEN].copy_from_slice(&primary);
        assert_eq!(decode_meta(&b).unwrap(), (v(10), MetaCopy::Primary));
    }
    let mut b = vec![0u8; 4096];
    b[..META_LEN].copy_from_slice(&primary);
    b[META_LEN..2 * META_LEN].copy_from_slice(&encode_meta(&vn(10), MetaCopy::Replica));
    assert_eq!(decode_meta(&b).unwrap(), (v(10), MetaCopy::Primary));
    // A block shorter than the record.
    for len in 0..META_LEN {
        assert!(is_corrupt(&decode_meta(&primary[..len])), "{len} bytes");
    }
    // Page 0 is not a meta.
    assert!(is_corrupt(&decode_meta(&encode_page0(&own()))));
    // `Ref`: 16 bytes, slot, epoch, crc, little-endian; slot 0 is null.
    let r = Ref {
        slot: 0x0807_0605_0403_0201,
        epoch: 0x0C0B_0A09,
        crc: 0x100F_0E0D,
    };
    let want: Vec<u8> = (1..=16).collect();
    assert_eq!(r.encode().to_vec(), want);
    assert_eq!(Ref::decode(&r.encode()), r);
    assert_eq!(Ref::default(), Ref::NULL);
    assert_eq!(Ref::NULL.encode(), [0u8; 16]);
    assert!(Ref::NULL.is_null());
    assert!(Ref { slot: 0, ..r }.is_null());
    assert!(!r.is_null());
    assert_eq!(primary[66..82], v(10).map_root.encode());
    assert_eq!(primary[83..99], v(10).free_root.encode());
}

#[test]
fn meta_every_byte_under_crc() {
    let rec = encode_meta(&v(10), MetaCopy::Primary);
    for i in 0..META_LEN {
        for bit in 0..8 {
            let mut b = rec;
            b[i] ^= 1 << bit;
            assert!(
                is_corrupt(&decode_meta(&b)),
                "byte {i} with bit {bit} flipped decodes: the crc does not cover it"
            );
        }
    }
}

#[test]
fn meta_malformed_fields() {
    // The copy byte, re-sealed with a correct crc.
    for copy in [2u8, 255] {
        let mut b = encode_meta(&v(10), MetaCopy::Primary);
        b[COPY_AT] = copy;
        reseal(&mut b, META_CRC_AT);
        assert!(is_corrupt(&decode_meta(&b)), "copy byte {copy} decodes");
    }
    // Each out-of-domain field, written by the encoder, whose crc is
    // correct, so the domain check is what refuses it.
    let malformed = [
        (
            "map_level 0",
            Meta {
                map_level: 0,
                ..v(10)
            },
        ),
        ("epoch != ckpt_id as u32", Meta { epoch: 11, ..v(10) }),
        (
            "format_version 2",
            Meta {
                identity: Identity {
                    format_version: 2,
                    ..own()
                },
                ..v(10)
            },
        ),
        (
            "page_size 12288",
            Meta {
                identity: Identity {
                    page_size: 12288,
                    ..own()
                },
                ..v(10)
            },
        ),
        (
            "checksum_alg 2",
            Meta {
                identity: Identity {
                    checksum_alg: 2,
                    ..own()
                },
                ..v(10)
            },
        ),
        (
            "wal_seg_size 0",
            Meta {
                identity: Identity {
                    wal_seg_size: 0,
                    ..own()
                },
                ..v(10)
            },
        ),
    ];
    for (what, m) in malformed {
        for copy in [MetaCopy::Primary, MetaCopy::Replica] {
            let b = encode_meta(&m, copy);
            assert_eq!(u32_at(&b, META_CRC_AT), crc32c(0, &b[..META_CRC_AT]));
            assert!(is_corrupt(&decode_meta(&b)), "{what} decodes");
        }
    }
    // The epoch is the low 32 bits of ckpt_id: the high bits are not
    // compared.
    let wide = Meta {
        ckpt_id: (7 << 32) + 10,
        ..v(10)
    };
    assert_eq!(
        decode_meta(&encode_meta(&wide, MetaCopy::Primary)).unwrap(),
        (wide, MetaCopy::Primary)
    );
    // The range check is the open's (§14 step 1, A-7d), not the decoder's.
    let t_above = Meta { t: 2000, ..v(10) };
    assert!(t_above.t > t_above.ts_ckpt_hi);
    assert_eq!(
        decode_meta(&encode_meta(&t_above, MetaCopy::Replica)).unwrap(),
        (t_above, MetaCopy::Replica)
    );
    let start_above = Meta {
        wal_start_lsn: v(10).wal_ckpt_end + 1,
        ..v(10)
    };
    assert_eq!(
        decode_meta(&encode_meta(&start_above, MetaCopy::Primary)).unwrap(),
        (start_above, MetaCopy::Primary)
    );
}

#[test]
fn replica_region() {
    assert_eq!(REPLICA_OFFSETS, [1_048_576, 1_052_672]);
    assert_eq!(REPLICA_BLOCK_LEN, 4096);
    assert_eq!(PAGE_SIZES, [4096, 8192, 16384, 32768, 65536]);
    let want = [
        (4096u32, 256u64..258u64, 258u64),
        (8192, 128..129, 129),
        (16384, 64..65, 65),
        (32768, 32..33, 33),
        (65536, 16..17, 17),
    ];
    for (ps, reserved, min) in want {
        assert!(is_page_size(ps));
        assert_eq!(reserved_slots(ps).unwrap(), reserved, "page size {ps}");
        assert_eq!(min_file_slots(ps).unwrap(), min, "page size {ps}");
        // Every reserved slot lies in [3, min_file_slots).
        assert!(reserved.clone().all(|s| (3..min).contains(&s)));
        // The two replica blocks lie inside the reserved slots' bytes.
        let (lo, hi) = (reserved.start * u64::from(ps), reserved.end * u64::from(ps));
        for off in REPLICA_OFFSETS {
            assert!(
                lo <= off && off + REPLICA_BLOCK_LEN as u64 <= hi,
                "page size {ps}"
            );
        }
        // And every reserved slot overlaps the region [1 MiB, 1 MiB + 8 KiB).
        for s in reserved {
            let (start, end) = (s * u64::from(ps), (s + 1) * u64::from(ps));
            assert!(start < 1_056_768 && end > 1_048_576, "slot {s} at {ps}");
        }
    }
    for ps in [0, 1, 512, 2048, 4095, 12288, 131_072, u32::MAX] {
        assert!(!is_page_size(ps));
        assert!(is_invalid(&reserved_slots(ps)), "page size {ps}");
        assert!(is_invalid(&min_file_slots(ps)), "page size {ps}");
    }
    // The meta places (§2.2): parity 0 in slot 1, parity 1 in slot 2.
    for (k, parity, slot) in [
        (0u64, 0usize, 1u64),
        (1, 1, 2),
        (9, 1, 2),
        (10, 0, 1),
        (u64::MAX, 1, 2),
    ] {
        assert_eq!(
            (meta_parity(k), meta_slot(k)),
            (parity, slot),
            "ckpt_id {k}"
        );
    }
}

#[test]
fn boot_id_order() {
    assert!(boot_id_older(5, 6));
    assert!(!boot_id_older(6, 5));
    // Wrapped: 0xFFFF_FFF0 is older than 2.
    assert!(boot_id_older(0xFFFF_FFF0, 2));
    assert!(!boot_id_older(2, 0xFFFF_FFF0));
    // Exactly 2^31 apart: each is older than the other.
    for a in [0u32, 7, 0xFFFF_FFF0] {
        let b = a.wrapping_add(1 << 31);
        assert!(boot_id_older(a, b) && boot_id_older(b, a), "{a} and {b}");
    }
    // One less than 2^31 apart: one direction only.
    assert!(boot_id_older(0, (1 << 31) - 1));
    assert!(!boot_id_older((1 << 31) - 1, 0));
    // Equal ids are not older.
    for a in [0u32, 5, 1 << 31, u32::MAX] {
        assert!(!boot_id_older(a, a), "{a}");
    }
}

// ---------------------------------------------------------------------
// The open rule
// ---------------------------------------------------------------------

#[test]
fn open_locate_page_size() {
    let page0 = encode_page0(&own());
    let mut torn = page0;
    torn[TORN_PAGE0_BYTE] ^= 0xFF;
    let r0 = encode_meta(&v(10), MetaCopy::Replica);
    let r1 = encode_meta(&v(9), MetaCopy::Replica);
    let at8k = Meta {
        identity: Identity {
            page_size: 8192,
            ..own()
        },
        ..v(9)
    };
    let r1_at8k = encode_meta(&at8k, MetaCopy::Replica);
    let foreign_at8k = Meta {
        identity: Identity {
            page_size: 8192,
            ..identity_with(FOREIGN_UUID)
        },
        ..base(15, 5, 0)
    };
    let r1_foreign_at8k = encode_meta(&foreign_at8k, MetaCopy::Replica);
    let mut torn_meta = r0;
    torn_meta[TORN_META_BYTE] ^= 0xFF;
    let zeros = [0u8; META_LEN];
    let (read, unreadable) = (Block::Read, Block::Unreadable);

    // Page 0 valid gives its size, whatever the replicas hold.
    assert_eq!(
        locate_page_size(&read(&page0), &[unreadable, unreadable]),
        Some(4096)
    );
    let page0_at8k = encode_page0(&Identity {
        page_size: 8192,
        ..own()
    });
    assert_eq!(
        locate_page_size(&read(&page0_at8k), &[read(&r0), read(&r1)]),
        Some(8192)
    );
    // Page 0 torn, one decoding replica: that copy's size.
    assert_eq!(
        locate_page_size(&read(&torn), &[read(&r0), unreadable]),
        Some(4096)
    );
    assert_eq!(
        locate_page_size(&unreadable, &[read(&zeros), read(&r1_at8k)]),
        Some(8192)
    );
    // Two decoding replicas that agree: their size.
    assert_eq!(
        locate_page_size(&read(&torn), &[read(&r0), read(&r1)]),
        Some(4096)
    );
    // Two that disagree (one foreign-shaped at 8192): none.
    assert_eq!(
        locate_page_size(&read(&torn), &[read(&r0), read(&r1_foreign_at8k)]),
        None
    );
    // No decoding replica: none.
    assert_eq!(
        locate_page_size(&read(&torn), &[read(&torn_meta), read(&zeros)]),
        None
    );
    assert_eq!(
        locate_page_size(&unreadable, &[unreadable, unreadable]),
        None
    );
}

/// A symbol of `t_meta_copies`' table form (card A-2-0 Work 6, part 3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Sym {
    /// `V(c)`, the place's own copy byte.
    V,
    /// `V(c)` with the other place's copy byte.
    X,
    /// `V(c − 2)`: a valid stale copy.
    S,
    /// `V(c)`'s bytes with byte 50 flipped: torn, the crc fails.
    T,
    /// 167 zero bytes.
    Z,
    /// An unreadable place.
    U,
    /// A misplaced copy one (parity 0) or two (parity 1) above 10.
    Mh,
    /// A misplaced copy below (parity 0) or equal to (parity 1) 10.
    Ml,
    /// A foreign copy with a higher `ckpt_id`.
    F,
    /// `Vn(c)`: a newer `boot_id`.
    Vn,
    /// `E(c)`: an equal `boot_id`, another field differing.
    E,
}

/// The eleven symbols.
const ALPHABET: [Sym; 11] = [
    Sym::V,
    Sym::X,
    Sym::S,
    Sym::T,
    Sym::Z,
    Sym::U,
    Sym::Mh,
    Sym::Ml,
    Sym::F,
    Sym::Vn,
    Sym::E,
];

/// Page 0's state in a case.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Page0 {
    /// The fixture's page 0.
    Valid,
    /// Byte 20 flipped: the crc fails.
    Torn,
    /// Its read failed.
    Unreadable,
}

/// The four places in the spot cases' order: 0 = P0 (slot 1), 1 = R0
/// (parity 0's replica), 2 = P1 (slot 2), 3 = R1 (parity 1's replica).
/// A place's parity.
fn parity_of(place: usize) -> u64 {
    (place / 2) as u64
}

/// Whether a place is a slot (a primary place), not a replica block.
fn is_slot(place: usize) -> bool {
    place.is_multiple_of(2)
}

/// The copy byte written for a place, and for its parity's other place.
fn copies_for(place: usize) -> (MetaCopy, MetaCopy) {
    if is_slot(place) {
        (MetaCopy::Primary, MetaCopy::Replica)
    } else {
        (MetaCopy::Replica, MetaCopy::Primary)
    }
}

/// The meta a symbol's copy carries at `place`, from the fixture; `None`
/// for the symbols whose bytes hold no copy (T, Z, U).
fn sym_meta(s: Sym, place: usize) -> Option<Meta> {
    let q = parity_of(place);
    let c = if q == 0 { 10 } else { 9 };
    match s {
        Sym::V | Sym::X => Some(v(c)),
        Sym::S => Some(v(c - 2)),
        Sym::T | Sym::Z | Sym::U => None,
        Sym::Mh => Some(v(if q == 0 { 11 } else { 12 })),
        Sym::Ml => Some(v(if q == 0 { 9 } else { 10 })),
        Sym::F => Some(foreign(if q == 0 { 14 } else { 15 })),
        Sym::Vn => Some(vn(c)),
        Sym::E => Some(e(c)),
    }
}

/// The bytes a symbol puts at `place`; `None` for an unreadable place.
fn sym_bytes(s: Sym, place: usize) -> Option<Vec<u8>> {
    let q = parity_of(place);
    let c = if q == 0 { 10 } else { 9 };
    let (own_copy, other_copy) = copies_for(place);
    match s {
        Sym::U => None,
        Sym::Z => Some(vec![0u8; META_LEN]),
        Sym::X => Some(encode_meta(&v(c), other_copy).to_vec()),
        Sym::T => {
            let mut b = encode_meta(&v(c), own_copy);
            b[TORN_META_BYTE] ^= 0xFF;
            Some(b.to_vec())
        }
        Sym::V | Sym::S | Sym::Mh | Sym::Ml | Sym::F | Sym::Vn | Sym::E => {
            sym_meta(s, place).map(|m| encode_meta(&m, own_copy).to_vec())
        }
    }
}

/// The bytes of every symbol at every place and of page 0, built once.
struct Fixture {
    /// `places[(place, symbol)]`: the bytes, or `None` for unreadable.
    places: BTreeMap<(usize, Sym), Option<Vec<u8>>>,
    /// The valid page 0.
    page0: Vec<u8>,
    /// Page 0 with byte 20 flipped.
    page0_torn: Vec<u8>,
}

impl Fixture {
    fn new() -> Fixture {
        let mut places = BTreeMap::new();
        for place in 0..4 {
            for s in ALPHABET {
                places.insert((place, s), sym_bytes(s, place));
            }
        }
        let page0 = encode_page0(&own()).to_vec();
        let mut page0_torn = page0.clone();
        page0_torn[TORN_PAGE0_BYTE] ^= 0xFF;
        Fixture {
            places,
            page0,
            page0_torn,
        }
    }

    /// Runs one case as the real open would: page 0 and the replica blocks
    /// first, `slots` from `locate_page_size`, then `select_meta`.
    fn run(&self, syms: [Sym; 4], page0: Page0) -> OpenDecision {
        let block = |place: usize| match self.places.get(&(place, syms[place])) {
            Some(Some(b)) => Block::Read(b.as_slice()),
            _ => Block::Unreadable,
        };
        let page0 = match page0 {
            Page0::Valid => Block::Read(&self.page0),
            Page0::Torn => Block::Read(&self.page0_torn),
            Page0::Unreadable => Block::Unreadable,
        };
        let replicas = [block(1), block(3)];
        let slots = locate_page_size(&page0, &replicas).map(|_| [block(0), block(2)]);
        select_meta(&HeaderBlocks {
            page0,
            replicas,
            slots,
        })
    }
}

/// An outcome of the open rule, as the oracle states it.
#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)] // a test value, built and compared once per case
enum Outcome {
    /// The newest and the previous meta, in full.
    Open {
        newest: Meta,
        previous: Option<Meta>,
    },
    /// 58030.
    AllUnreadable,
    /// XX001; the reason is not compared.
    Corrupt,
}

/// `select_meta`'s decision as an outcome.
fn outcome_of(d: &OpenDecision) -> Outcome {
    match d {
        OpenDecision::Open(sel) => Outcome::Open {
            newest: sel.newest.clone(),
            previous: sel.previous.clone(),
        },
        OpenDecision::AllUnreadable => Outcome::AllUnreadable,
        OpenDecision::Corrupt(_) => Outcome::Corrupt,
    }
}

/// Whether an `Open`'s identity is its newest meta's, as it must be: a
/// valid copy has the file's identity (§2.2).
fn identity_is_newest(d: &OpenDecision) -> bool {
    match d {
        OpenDecision::Open(sel) => sel.identity == sel.newest.identity,
        OpenDecision::AllUnreadable | OpenDecision::Corrupt(_) => true,
    }
}

/// §12.5's serial-number order, written again here so that the oracle
/// does not lean on the crate.
fn older(a: u32, b: u32) -> bool {
    a.wrapping_sub(b) >= 1 << 31
}

/// §2.2's outcome for a case, computed from the symbols, never by decoding
/// bytes (card A-2-0 Work 6). It knows what each symbol holds: whether its
/// bytes are a copy, and the copy's meta (its `ckpt_id`, identity,
/// `boot_id` and content), from the fixture.
fn oracle(syms: [Sym; 4], page0: Page0) -> Outcome {
    let held: [Option<Meta>; 4] = std::array::from_fn(|place| sym_meta(syms[place], place));
    let page0_valid = page0 == Page0::Valid;
    // "the replica copies give the page size, and so the offsets of slots 1
    // and 2": every copy in the alphabet is at 4096-byte pages, foreign
    // ones included, so a replica copy locates the slots.
    let located = page0_valid || held[1].is_some() || held[3].is_some();
    // "if every place's read fails, the open fails with 58030".
    if located && syms.iter().all(|&s| s == Sym::U) {
        return Outcome::AllUnreadable;
    }
    // The copies the open sees: a slot's only when the slots are located.
    let seen: [Option<Meta>; 4] = std::array::from_fn(|place| {
        if is_slot(place) && !located {
            None
        } else {
            held[place].clone()
        }
    });
    // "The file's identity is page 0's fields ... when page 0's magic and
    // CRC are valid. Otherwise ... if neither [replica block] holds a copy
    // ..., the open fails with XX001; ... the identity is then the fields
    // that every copy ... shares; if two of them differ ..., XX001."
    let identity = if page0_valid {
        own()
    } else {
        if seen[1].is_none() && seen[3].is_none() {
            return Outcome::Corrupt;
        }
        let mut ids: Vec<Identity> = seen.iter().flatten().map(|m| m.identity).collect();
        ids.dedup();
        match ids.as_slice() {
            [one] => *one,
            _ => return Outcome::Corrupt,
        }
    };
    // "A valid copy has valid magic and CRC and the file's identity, and
    // sits in a place of its own parity"; another identity is foreign,
    // ignored; the other parity's place makes it misplaced.
    let mut misplaced = Vec::new();
    let mut valid: [Vec<Meta>; 2] = [Vec::new(), Vec::new()];
    for (place, copy) in seen.iter().enumerate() {
        let Some(m) = copy else { continue };
        if m.identity != identity {
            continue;
        }
        let q = parity_of(place);
        if m.ckpt_id % 2 == q {
            valid[q as usize].push(m.clone());
        } else {
            misplaced.push(m.ckpt_id);
        }
    }
    // Each parity's candidate, by §2.2's table.
    let [valid0, valid1] = valid;
    let (Some(c0), Some(c1)) = (table_candidate(&valid0), table_candidate(&valid1)) else {
        return Outcome::Corrupt;
    };
    // "The newest meta k is the candidate with the higher ckpt_id. If there
    // is none, the open fails with XX001."
    let (newest, other) = match (c0, c1) {
        (None, None) => return Outcome::Corrupt,
        (Some(a), None) | (None, Some(a)) => (a, None),
        (Some(a), Some(b)) => {
            if a.ckpt_id > b.ckpt_id {
                (a, Some(b))
            } else {
                (b, Some(a))
            }
        }
    };
    // "If a misplaced copy's ckpt_id exceeds k + 1, the open fails with
    // XX001."
    if misplaced.iter().any(|&c| c > newest.ckpt_id + 1) {
        return Outcome::Corrupt;
    }
    // "The previous meta is the other parity's candidate when its ckpt_id
    // is k−1."
    let previous = other.filter(|m| m.ckpt_id + 1 == newest.ckpt_id);
    Outcome::Open { newest, previous }
}

/// One parity's candidate from its valid copies, the primary place's first,
/// by §2.2's table and the tie rule: `Some(None)` for none, `None` for the
/// tie rule's XX001.
fn table_candidate(valid: &[Meta]) -> Option<Option<Meta>> {
    let candidate = match valid {
        [] => None,
        [m] => Some(m.clone()),
        [p, r] if p.ckpt_id > r.ckpt_id => Some(p.clone()),
        [p, r] if p.ckpt_id < r.ckpt_id => Some(r.clone()),
        [p, r] if p == r => Some(p.clone()),
        // The tie rule: the copy with the newer boot_id; neither newer: XX001.
        [p, r] if older(r.boot_id, p.boot_id) && !older(p.boot_id, r.boot_id) => Some(p.clone()),
        [p, r] if older(p.boot_id, r.boot_id) && !older(r.boot_id, p.boot_id) => Some(r.clone()),
        _ => return None,
    };
    Some(candidate)
}

/// `Open { newest, previous }`.
fn open(newest: Meta, previous: Option<Meta>) -> Outcome {
    Outcome::Open { newest, previous }
}

#[test]
fn t_meta_copies() {
    use Sym::{E, F, Mh, Ml, S, T, U, V, Vn, X, Z};
    let fx = Fixture::new();

    // Part 1: the spot cases ([P0, R0, P1, R1], page 0), each anchoring the
    // oracle too.
    let spots: Vec<([Sym; 4], Page0, Outcome)> = vec![
        ([V, V, V, V], Page0::Valid, open(v(10), Some(v(9)))),
        ([Vn, V, V, V], Page0::Valid, open(vn(10), Some(v(9)))),
        // TIE-NO-BOOT-ID's case: the newer boot_id in the replica.
        ([V, Vn, V, V], Page0::Valid, open(vn(10), Some(v(9)))),
        // TIE-EQUAL-OPENS's case, and the same tie in the previous meta's
        // parity.
        ([V, E, V, V], Page0::Valid, Outcome::Corrupt),
        ([V, V, E, V], Page0::Valid, Outcome::Corrupt),
        ([T, T, V, V], Page0::Valid, open(v(9), None)),
        ([S, S, V, V], Page0::Valid, open(v(9), Some(v(8)))),
        // MISPLACED-IGNORED's case (12 > 10 + 1) and
        // MISPLACED-NEXT-REFUSES's (11 = 10 + 1).
        ([V, V, V, Mh], Page0::Valid, Outcome::Corrupt),
        ([V, Mh, V, V], Page0::Valid, open(v(10), Some(v(9)))),
        ([T, Mh, V, V], Page0::Valid, Outcome::Corrupt),
        ([T, T, Mh, V], Page0::Valid, Outcome::Corrupt),
        // MISPLACED-SHADOWS's case: a misplaced copy equal to the newest;
        // and one below it.
        ([V, V, V, Ml], Page0::Valid, open(v(10), Some(v(9)))),
        ([Ml, V, V, V], Page0::Valid, open(v(10), Some(v(9)))),
        // IDENTITY-SKIP's case; with page 0 torn, an identity conflict.
        ([V, V, V, F], Page0::Valid, open(v(10), Some(v(9)))),
        ([V, V, V, F], Page0::Torn, Outcome::Corrupt),
        // PAGE0-REQUIRED's case.
        ([V, V, V, V], Page0::Torn, open(v(10), Some(v(9)))),
        ([V, V, V, V], Page0::Unreadable, open(v(10), Some(v(9)))),
        // 58030; with page 0 torn the slots are not located (corner (b)).
        ([U, U, U, U], Page0::Valid, Outcome::AllUnreadable),
        ([U, U, U, U], Page0::Torn, Outcome::Corrupt),
        // Corner (a): every decoding copy foreign to one identity.
        ([T, F, T, T], Page0::Torn, open(foreign(14), None)),
        ([Z, Z, Z, Z], Page0::Valid, Outcome::Corrupt),
        ([V, T, V, T], Page0::Torn, Outcome::Corrupt),
        ([V, X, V, V], Page0::Valid, open(v(10), Some(v(9)))),
        ([U, V, U, U], Page0::Valid, open(v(10), None)),
        // PREV-ANY-PARITY's case (card mutant 10): 7 is not 10 − 1.
        ([V, V, S, S], Page0::Valid, open(v(10), None)),
    ];
    for (syms, page0, want) in &spots {
        let got = fx.run(*syms, *page0);
        assert_eq!(
            &outcome_of(&got),
            want,
            "spot case {syms:?}, page 0 {page0:?}: {got:?}"
        );
        assert_eq!(
            &oracle(*syms, *page0),
            want,
            "the oracle on spot case {syms:?}, page 0 {page0:?}"
        );
        assert!(
            identity_is_newest(&got),
            "spot case {syms:?}, page 0 {page0:?}: {got:?}"
        );
    }

    // Part 2: ties in serial-number order, outside the cross product.
    let page0 = encode_page0(&own());
    let parity1 = [
        encode_meta(&v(9), MetaCopy::Primary),
        encode_meta(&v(9), MetaCopy::Replica),
    ];
    let tie = |primary: &Meta, replica: &Meta| -> OpenDecision {
        let p = encode_meta(primary, MetaCopy::Primary);
        let r = encode_meta(replica, MetaCopy::Replica);
        let replicas = [Block::Read(&r), Block::Read(&parity1[1])];
        let slots = locate_page_size(&Block::Read(&page0), &replicas)
            .map(|_| [Block::Read(&p[..]), Block::Read(&parity1[0][..])]);
        select_meta(&HeaderBlocks {
            page0: Block::Read(&page0),
            replicas,
            slots,
        })
    };
    // 2 is newer than 0xFFFF_FFF0 in §12.5's order; a numeric comparison
    // would pick the primary.
    let (wrapped, after_wrap) = (base(10, 0xFFFF_FFF0, 0), base(10, 2, 1));
    assert_eq!(
        outcome_of(&tie(&wrapped, &after_wrap)),
        open(after_wrap.clone(), Some(v(9)))
    );
    assert_eq!(
        outcome_of(&tie(&after_wrap, &wrapped)),
        open(after_wrap.clone(), Some(v(9)))
    );
    // Two ids exactly 2^31 apart: neither is newer, XX001.
    for (a, b) in [(0xFFFF_FFF0u32, 0x7FFF_FFF0u32), (5, 5 + (1 << 31))] {
        let d = tie(&base(10, a, 0), &base(10, b, 1));
        assert_eq!(
            d,
            OpenDecision::Corrupt(OpenCorrupt::EqualBootIds { parity: 0 }),
            "{a:#x} and {b:#x}"
        );
        let d = tie(&base(10, b, 1), &base(10, a, 0));
        assert_eq!(outcome_of(&d), Outcome::Corrupt, "{b:#x} and {a:#x}");
    }

    // Part 3: the cross product, 11^4 symbol combinations with page 0
    // valid, torn and unreadable, each against the oracle.
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for page0 in [Page0::Valid, Page0::Torn, Page0::Unreadable] {
        for a in ALPHABET {
            for b in ALPHABET {
                for c in ALPHABET {
                    for d in ALPHABET {
                        let syms = [a, b, c, d];
                        let got = fx.run(syms, page0);
                        let want = oracle(syms, page0);
                        assert_eq!(
                            outcome_of(&got),
                            want,
                            "case {syms:?}, page 0 {page0:?}: select_meta gave {got:?}"
                        );
                        assert!(
                            identity_is_newest(&got),
                            "case {syms:?}, page 0 {page0:?}: {got:?}"
                        );
                        let class = match want {
                            Outcome::Open { .. } => "Open",
                            Outcome::AllUnreadable => "AllUnreadable",
                            Outcome::Corrupt => "Corrupt",
                        };
                        *counts.entry(class).or_default() += 1;
                    }
                }
            }
        }
    }
    let total: usize = counts.values().sum();
    println!("t_meta_copies cross product: {total} cases, {counts:?}");
    assert_eq!(total, 43_923);
    for class in ["Open", "AllUnreadable", "Corrupt"] {
        assert!(
            counts.get(class).copied().unwrap_or(0) >= 1,
            "no {class} in the cross product"
        );
    }
}

// ---------------------------------------------------------------------
// The fuzz targets (libFuzzer-shaped, `fn(&[u8])`; card A-2-0 Work 6)
// ---------------------------------------------------------------------

/// Page 0's decoder: never panics, refuses with `Corrupt` only, and
/// accepts only canonical records.
fn fuzz_page0(data: &[u8]) -> Result<(), String> {
    match decode_page0(data) {
        Ok(id) => {
            let back = encode_page0(&id);
            if back.get(..PAGE0_RECORD_LEN) == data.get(..PAGE0_RECORD_LEN) {
                Ok(())
            } else {
                Err(format!(
                    "decode_page0 accepted {} as {id:?}, which encodes to {}",
                    hex(data.get(..PAGE0_RECORD_LEN).unwrap_or(data)),
                    hex(back.get(..PAGE0_RECORD_LEN).unwrap_or(&back))
                ))
            }
        }
        Err(StoreError::Corrupt(_)) => Ok(()),
        Err(other) => Err(format!("decode_page0 failed with {other:?}, not Corrupt")),
    }
}

/// The meta decoder: never panics, refuses with `Corrupt` only, and accepts
/// only canonical records.
fn fuzz_meta(data: &[u8]) -> Result<(), String> {
    match decode_meta(data) {
        Ok((m, c)) => {
            let back = encode_meta(&m, c);
            if data.get(..META_LEN) == Some(&back[..]) {
                Ok(())
            } else {
                Err(format!(
                    "decode_meta accepted {} as {m:?} {c:?}, which encodes to {}",
                    hex(data.get(..META_LEN).unwrap_or(data)),
                    hex(&back)
                ))
            }
        }
        Err(StoreError::Corrupt(_)) => Ok(()),
        Err(other) => Err(format!("decode_meta failed with {other:?}, not Corrupt")),
    }
}

/// The open rule: a flags byte (bit `i` set: block `i` is unreadable) and
/// five 167-byte blocks, zero-padded, in the order page 0, parity 0's
/// replica, parity 1's replica, slot 1, slot 2; `slots` from
/// `locate_page_size`. On `Open`: a previous meta is `newest − 1`, the
/// newest is held in a place of its own parity, and the identity is the
/// newest's.
fn fuzz_open(data: &[u8]) -> Result<(), String> {
    let flags = data.first().copied().unwrap_or(0);
    let rest = data.get(1..).unwrap_or(&[]);
    let mut blocks = [[0u8; META_LEN]; 5];
    for (i, block) in blocks.iter_mut().enumerate() {
        for (d, s) in block.iter_mut().zip(rest.iter().skip(i * META_LEN)) {
            *d = *s;
        }
    }
    let block = |i: usize| {
        if flags & (1 << i) != 0 {
            Block::Unreadable
        } else {
            Block::Read(&blocks[i][..])
        }
    };
    let page0 = block(0);
    let replicas = [block(1), block(2)];
    let slots = locate_page_size(&page0, &replicas).map(|_| [block(3), block(4)]);
    let decision = select_meta(&HeaderBlocks {
        page0,
        replicas,
        slots,
    });
    let OpenDecision::Open(sel) = decision else {
        return Ok(());
    };
    if let Some(prev) = &sel.previous
        && prev.ckpt_id.checked_add(1) != Some(sel.newest.ckpt_id)
    {
        return Err(format!(
            "previous {} is not newest {} − 1",
            prev.ckpt_id, sel.newest.ckpt_id
        ));
    }
    let q = (sel.newest.ckpt_id % 2) as usize;
    let own_places = [Some(replicas[q]), slots.map(|s| s[q])];
    let held = own_places.iter().flatten().any(|b| match b {
        Block::Read(bytes) => decode_meta(bytes).is_ok_and(|(m, _)| m == sel.newest),
        Block::Unreadable => false,
    });
    if !held {
        return Err(format!(
            "newest {} is held in no place of parity {q}",
            sel.newest.ckpt_id
        ));
    }
    if sel.identity != sel.newest.identity {
        return Err(format!("identity {:?} is not the newest's", sel.identity));
    }
    Ok(())
}

/// All three targets on one input.
fn fuzz_all(data: &[u8]) -> Result<(), String> {
    fuzz_page0(data)?;
    fuzz_meta(data)?;
    fuzz_open(data)
}

/// A mutation of a corpus record: 1–4 byte overwrites, a truncation or an
/// extension (card A-2-0 Work 6).
#[derive(Clone, Debug)]
enum Mutation {
    /// Bytes written at positions within the record.
    Overwrite(Vec<(Index, u8)>),
    /// The record cut to a length below its own.
    Truncate(Index),
    /// Bytes appended.
    Extend(Vec<u8>),
}

/// The mutations, overwrites weighted up.
fn mutation() -> impl Strategy<Value = Mutation> {
    prop_oneof![
        4 => proptest::collection::vec((any::<Index>(), any::<u8>()), 1..=4).prop_map(Mutation::Overwrite),
        1 => any::<Index>().prop_map(Mutation::Truncate),
        1 => proptest::collection::vec(any::<u8>(), 1..=64).prop_map(Mutation::Extend),
    ]
}

/// `rec` mutated, then, when `reseal`, re-sealed with a correct crc at
/// `crc_at` so that the domain checks are reached.
fn mutate(rec: &[u8], m: &Mutation, reseal_it: bool, crc_at: usize) -> Vec<u8> {
    let mut out = rec.to_vec();
    match m {
        Mutation::Overwrite(writes) => {
            for (at, byte) in writes {
                let i = at.index(out.len());
                out[i] = *byte;
            }
        }
        Mutation::Truncate(at) => out.truncate(at.index(out.len())),
        Mutation::Extend(tail) => out.extend_from_slice(tail),
    }
    if reseal_it {
        reseal(&mut out, crc_at);
    }
    out
}

/// The mutated driver's corpus of meta-place bytes: every symbol of part 3
/// that has bytes, at every place.
fn meta_corpus() -> Vec<Vec<u8>> {
    (0..4)
        .flat_map(|place| {
            ALPHABET
                .into_iter()
                .filter_map(move |s| sym_bytes(s, place))
        })
        .collect()
}

/// The first 167 bytes of `rec`, zero-padded.
fn padded(rec: &[u8]) -> Vec<u8> {
    let mut out = vec![0u8; META_LEN];
    for (d, s) in out.iter_mut().zip(rec) {
        *d = *s;
    }
    out
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// Arbitrary bytes into all three targets.
    #[test]
    fn meta_fuzz_bytes(data in proptest::collection::vec(any::<u8>(), 0..600)) {
        fuzz_all(&data).map_err(TestCaseError::fail)?;
    }

    /// The corpus (page 0's record and every symbol of part 3), mutated
    /// and in half the cases re-sealed, into the decoders one record at a
    /// time, and five of them at once into the open rule.
    #[test]
    fn meta_fuzz_mutated(
        page0 in (mutation(), any::<bool>()),
        places in proptest::array::uniform4((any::<Index>(), mutation(), any::<bool>())),
        flags in any::<u8>(),
    ) {
        let page0_record = encode_page0(&own())[..PAGE0_RECORD_LEN].to_vec();
        let corpus = meta_corpus();
        let mut records = vec![mutate(&page0_record, &page0.0, page0.1, PAGE0_CRC_AT)];
        for (pick, m, reseal_it) in &places {
            records.push(mutate(&corpus[pick.index(corpus.len())], m, *reseal_it, META_CRC_AT));
        }
        let mut open_input = vec![flags];
        for rec in &records {
            fuzz_page0(rec).map_err(TestCaseError::fail)?;
            fuzz_meta(rec).map_err(TestCaseError::fail)?;
            open_input.extend_from_slice(&padded(rec));
        }
        fuzz_open(&open_input).map_err(TestCaseError::fail)?;
    }
}
