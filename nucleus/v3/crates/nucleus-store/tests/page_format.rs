//! `page_format.rs` (card A-2): the page header and I-INTEGRITY (S-STORE
//! §3.1, §3.3), the size limits and I-KEY-SIZE (§3.2), the slotted body on
//! the new header, and the page map's pages and positions (§4.1).
//!
//! The oracles are this file's own, built from the spec's field lists and
//! the card's tables, not from the crate's encoders: a bitwise CRC32C, raw
//! little-endian offsets, and byte-level models of the header, the map
//! page and the B+tree header that decide acceptance independently.
//!
//! Every test is a pure function of bytes: no I/O, no `Env`, no thread, no
//! clock, no hook. Randomness is confined to the two fuzz drivers at the
//! end (`page_fuzz_bytes`, `page_fuzz_mutated`), which feed the four fuzz
//! targets of PLAN-v3 §6.2's "page decoder, page-map decoder"; every
//! mutant this card names is caught by a deterministic test above them.

use nucleus_store::StoreError;
use nucleus_store::map::{self, MapGeometry, Radix};
use nucleus_store::meta::Ref;
use nucleus_store::page::{self, Kind, PageHeader, PageKind};
use nucleus_store::store::PageId;
use proptest::prelude::*;

/// The allowed page sizes (S-STORE §2.1): every test runs at each.
const SIZES: [usize; 5] = [4096, 8192, 16384, 32768, 65536];

/// Lengths that are no allowed page size: below and inside the header,
/// one byte either side of an allowed size, and sizes between and beyond.
const BAD_LENS: [usize; 13] = [
    0, 1, 4, 23, 24, 44, 4095, 4097, 8191, 12288, 65535, 65537, 131072,
];

/// Work 3's table: page size, `U`, `Q`, `K_MAX`, `V_INLINE`, the maximum
/// leaf cell and the maximum inner cell (each with its slot).
const LIMITS: [[usize; 7]; 5] = [
    [4096, 4052, 1013, 334, 669, 1013, 348],
    [8192, 8148, 2037, 675, 1352, 2037, 689],
    [16384, 16340, 4085, 1358, 2717, 4085, 1372],
    [32768, 32724, 8181, 2723, 5448, 8181, 2737],
    [65536, 65492, 16373, 5454, 10909, 16373, 5468],
];

// ---------------------------------------------------------------------------
// Independent oracles
// ---------------------------------------------------------------------------

/// CRC32C, bitwise over the reflected Castagnoli polynomial 0x82F63B78:
/// independent of the crate's table-driven `crc32c`.
fn crc32c_bitwise(data: &[u8]) -> u32 {
    let mut c = !0u32;
    for &b in data {
        c ^= u32::from(b);
        for _ in 0..8 {
            c = if c & 1 != 0 {
                (c >> 1) ^ 0x82F6_3B78
            } else {
                c >> 1
            };
        }
    }
    !c
}

/// The little-endian u32 at `at`.
fn le_u32(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes([b[at], b[at + 1], b[at + 2], b[at + 3]])
}

/// The little-endian u64 at `at`.
fn le_u64(b: &[u8], at: usize) -> u64 {
    u64::from(le_u32(b, at)) | (u64::from(le_u32(b, at + 4)) << 32)
}

/// The 16 bytes of a ref: `slot u64, epoch u32, crc u32`, little-endian
/// (§2.2, §4.1).
fn ref_bytes(r: Ref) -> Vec<u8> {
    let mut v = r.slot.to_le_bytes().to_vec();
    v.extend_from_slice(&r.epoch.to_le_bytes());
    v.extend_from_slice(&r.crc.to_le_bytes());
    v
}

/// Lower-case hex of `b`.
fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// §3.1's header read from raw bytes by this file's own reading of the
/// spec: `Some((crc, kind byte, id, epoch))` exactly when the length is an
/// allowed page size, the kind is 3–9, and `flags` and the reserved bytes
/// 6..8 and 20..24 are zero.
fn oracle_header(page: &[u8]) -> Option<(u32, u8, u64, u32)> {
    if !SIZES.contains(&page.len()) {
        return None;
    }
    let kind = page[4];
    let zeros = page[5] == 0 && page[6..8] == [0, 0] && page[20..24] == [0; 4];
    ((3..=9).contains(&kind) && zeros)
        .then(|| (le_u32(page, 0), kind, le_u64(page, 8), le_u32(page, 16)))
}

/// §3.3's four comparisons for `page` read through `r` as `id`, from raw
/// bytes: the self CRC, the reference's crc, the id and the epoch. `page`
/// must be at least 24 bytes.
fn four_checks(page: &[u8], r: &Ref, id: u64) -> [bool; 4] {
    let stored = le_u32(page, 0);
    [
        stored == crc32c_bitwise(&page[4..]),
        stored == r.crc,
        le_u64(page, 8) == id,
        le_u32(page, 16) == r.epoch,
    ]
}

/// §4.1's map page at `(level, index)` with fanout `f`, judged from raw
/// bytes: the header decodes, the kind is 6 at level 0 and 7 above, the id
/// is `level << 56 | index`, every ref whose slot is 0 is all zero, and
/// every byte after the `f` refs is zero.
fn oracle_map_ok(page: &[u8], f: usize, level: u8, index: u64) -> bool {
    let Some((_, kind, id, _)) = oracle_header(page) else {
        return false;
    };
    let want_kind = if level == 0 { 6 } else { 7 };
    let end = 24 + 16 * f;
    kind == want_kind
        && id == ((u64::from(level) << 56) | index)
        && page[24..end]
            .chunks(16)
            .all(|r| le_u64(r, 0) != 0 || r.iter().all(|&b| b == 0))
        && page[end..].iter().all(|&b| b == 0)
}

/// The slotted body's B+tree header judged from raw bytes (card A-2's
/// table): `Some((kind byte, nslots, free_start, free_end))` exactly when
/// the page header decodes with kind 3 or 4, bytes 26..28 are zero and
/// `44 + 4·nslots ≤ free_start ≤ free_end ≤ len`.
fn oracle_slotted(page: &[u8]) -> Option<(u8, usize, usize, usize)> {
    let (_, kind, _, _) = oracle_header(page)?;
    let nslots = usize::from(u16::from_le_bytes([page[24], page[25]]));
    let free_start = le_u32(page, 28) as usize;
    let free_end = le_u32(page, 32) as usize;
    ((kind == 3 || kind == 4)
        && page[26..28] == [0, 0]
        && 44 + 4 * nslots <= free_start
        && free_start <= free_end
        && free_end <= page.len())
    .then_some((kind, nslots, free_start, free_end))
}

// ---------------------------------------------------------------------------
// Fixtures and assertions
// ---------------------------------------------------------------------------

/// The first 44 bytes `init` writes (card A-2's two tables): `crc` 0, the
/// kind, `flags` and reserved 0, `id` 0, `epoch` 0, reserved 0; `nslots`
/// 0, reserved 0, `free_start` 44, `free_end` the page size, right child 0.
fn init_bytes(ps: usize, kind: u8) -> Vec<u8> {
    let mut v = vec![0u8; 44];
    v[4] = kind;
    v[28..32].copy_from_slice(&44u32.to_le_bytes());
    v[32..36].copy_from_slice(&(ps as u32).to_le_bytes());
    v
}

/// A `len`-byte buffer of non-zero pattern bytes.
fn patterned(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i * 31 % 251 + 1) as u8).collect()
}

/// `data` cut or zero-padded to `len` bytes.
fn fit(data: &[u8], len: usize) -> Vec<u8> {
    let mut v: Vec<u8> = data.iter().copied().take(len).collect();
    v.resize(len, 0);
    v
}

/// A `len`-byte key: the big-endian counter `c`, then constant padding.
/// `len ≥ 4`, so keys order by `c` whatever their lengths.
fn ckey(c: u32, len: usize) -> Vec<u8> {
    let mut k = c.to_be_bytes().to_vec();
    k.resize(len, 0x33);
    k
}

/// A length in `lo..=hi` that varies with `i`, the maximum one in three.
fn mixed(i: u32, lo: usize, hi: usize) -> usize {
    if i.is_multiple_of(3) {
        hi
    } else {
        lo + (i as usize * 7919 + 13) % (hi - lo + 1)
    }
}

/// Assert `res` is `Corrupt` and its reason contains `needle`.
fn expect_corrupt<T: std::fmt::Debug>(res: Result<T, StoreError>, needle: &str, ctx: &str) {
    let msg = match &res {
        Err(StoreError::Corrupt(msg)) => msg.as_str(),
        _ => "",
    };
    assert!(
        matches!(res, Err(StoreError::Corrupt(_))),
        "{ctx}: expected Corrupt, got {res:?}"
    );
    assert!(
        msg.contains(needle),
        "{ctx}: Corrupt for another reason: {msg:?} (expected {needle:?})"
    );
}

/// Assert `res` is `Corrupt`, whatever its reason.
fn expect_any_corrupt<T: std::fmt::Debug>(res: Result<T, StoreError>, ctx: &str) {
    assert!(
        matches!(res, Err(StoreError::Corrupt(_))),
        "{ctx}: expected Corrupt, got {res:?}"
    );
}

/// Assert `res` is `Invalid`.
fn expect_invalid<T: std::fmt::Debug>(res: Result<T, StoreError>, ctx: &str) {
    assert!(
        matches!(res, Err(StoreError::Invalid(_))),
        "{ctx}: expected Invalid, got {res:?}"
    );
}

// ---------------------------------------------------------------------------
// §15.1's rows
// ---------------------------------------------------------------------------

/// §15.1 I-INTEGRITY, A-2, full (S-STORE §3.3): at every allowed page size
/// and for every kind 3–9, `verify` passes an intact page read through its
/// reference, and each of cases (b)–(e) fails exactly one of the four
/// comparisons (shown by the raw-byte oracle first), so each of CRC-SKIP,
/// REFCRC-SKIP, ID-SKIP and EPOCH-SKIP turns one case into `Ok`.
#[test]
fn row_i_integrity() {
    const ID: u64 = 0x0102_0304_0506_0708;
    for ps in SIZES {
        for kb in 3u8..=9 {
            let kind = Kind::from_byte(kb).unwrap();
            let ctx = format!("{ps} bytes, kind {kb}");
            let mut pg = patterned(ps);
            page::write_header(&mut pg, kind, ID, 11).unwrap();
            let crc = page::seal(&mut pg).unwrap();
            assert_eq!(crc, crc32c_bitwise(&pg[4..]), "{ctx}: seal");
            let r = Ref {
                slot: 77,
                epoch: 11,
                crc,
            };

            // (a) intact.
            assert_eq!(four_checks(&pg, &r, ID), [true; 4], "{ctx}: (a) fixture");
            let h = page::verify(&pg, &r, ID).unwrap();
            assert_eq!(
                h,
                PageHeader {
                    crc,
                    kind,
                    id: ID,
                    epoch: 11
                },
                "{ctx}: (a)"
            );

            // (b) self-CRC: one body byte flipped, header untouched.
            let mut b = pg.clone();
            b[24 + (usize::from(kb) * 1009) % (ps - 24)] ^= 0x01;
            assert_eq!(
                four_checks(&b, &r, ID),
                [false, true, true, true],
                "{ctx}: (b) fixture"
            );
            expect_corrupt(
                page::verify(&b, &r, ID),
                "CRC32C of its bytes",
                &format!("{ctx}: (b)"),
            );

            // (c) reference CRC: one body byte changed and the page
            // re-sealed; the reference still names the old crc.
            let mut c = pg.clone();
            c[ps - 1] ^= 0x40;
            let c_crc = page::seal(&mut c).unwrap();
            assert_eq!(c_crc, crc32c_bitwise(&c[4..]), "{ctx}: (c) seal");
            assert_eq!(
                four_checks(&c, &r, ID),
                [true, false, true, true],
                "{ctx}: (c) fixture"
            );
            expect_corrupt(
                page::verify(&c, &r, ID),
                "reference's crc",
                &format!("{ctx}: (c)"),
            );

            // (d) id: the page read as another LPID or position.
            assert_eq!(
                four_checks(&pg, &r, ID + 1),
                [true, true, false, true],
                "{ctx}: (d) fixture"
            );
            expect_corrupt(
                page::verify(&pg, &r, ID + 1),
                "expected id",
                &format!("{ctx}: (d)"),
            );

            // (e) epoch: a reference of another content epoch.
            let r12 = Ref { epoch: 12, ..r };
            assert_eq!(
                four_checks(&pg, &r12, ID),
                [true, true, true, false],
                "{ctx}: (e) fixture"
            );
            expect_corrupt(
                page::verify(&pg, &r12, ID),
                "reference's epoch",
                &format!("{ctx}: (e)"),
            );

            // (b2)-(e2) every bit of each compared field: a stored crc, a
            // reference crc, an expected id or a reference epoch that
            // differs in one bit fails that one check, so a check that
            // compares part of its field (a truncating cast, a mask) is
            // caught. In (b2) the reference names the altered stored crc,
            // so only the self CRC fails.
            for bit in 0..32 {
                let flip = 1u32 << bit;
                let mut b2 = pg.clone();
                b2[..4].copy_from_slice(&(crc ^ flip).to_le_bytes());
                let rb2 = Ref {
                    crc: crc ^ flip,
                    ..r
                };
                expect_corrupt(
                    page::verify(&b2, &rb2, ID),
                    "CRC32C of its bytes",
                    &format!("{ctx}: (b2) bit {bit}"),
                );
                expect_corrupt(
                    page::verify(&pg, &rb2, ID),
                    "reference's crc",
                    &format!("{ctx}: (c2) bit {bit}"),
                );
                let re2 = Ref {
                    epoch: 11 ^ flip,
                    ..r
                };
                expect_corrupt(
                    page::verify(&pg, &re2, ID),
                    "reference's epoch",
                    &format!("{ctx}: (e2) bit {bit}"),
                );
            }
            for bit in 0..64 {
                expect_corrupt(
                    page::verify(&pg, &r, ID ^ (1u64 << bit)),
                    "expected id",
                    &format!("{ctx}: (d2) bit {bit}"),
                );
            }
            // (d3), (e3) every bit of the page's own id and epoch: the page
            // with that field one bit off, re-sealed, and read through a
            // reference naming its new crc, fails that one check, so a
            // check that reads part of the page's field (a narrowed read, a
            // mask) is caught. (d2) and (e2) alter only the other side.
            for bit in 0..64 {
                let mut d3 = pg.clone();
                d3[8..16].copy_from_slice(&(ID ^ (1u64 << bit)).to_le_bytes());
                let rd3 = Ref {
                    crc: page::seal(&mut d3).unwrap(),
                    ..r
                };
                expect_corrupt(
                    page::verify(&d3, &rd3, ID),
                    "expected id",
                    &format!("{ctx}: (d3) bit {bit}"),
                );
            }
            for bit in 0..32 {
                let mut e3 = pg.clone();
                e3[16..20].copy_from_slice(&(11u32 ^ (1u32 << bit)).to_le_bytes());
                let re3 = Ref {
                    crc: page::seal(&mut e3).unwrap(),
                    ..r
                };
                expect_corrupt(
                    page::verify(&e3, &re3, ID),
                    "reference's epoch",
                    &format!("{ctx}: (e3) bit {bit}"),
                );
            }

            // (f) a page of a length outside PAGE_SIZES: cut by one byte,
            // grown by one byte, and every length of BAD_LENS.
            let mut grown = pg.clone();
            grown.push(0);
            let cut = &pg[..ps - 1];
            for bad in [cut, grown.as_slice()] {
                expect_corrupt(
                    page::verify(bad, &r, ID),
                    "not an allowed page size",
                    &format!("{ctx}: (f) {} bytes", bad.len()),
                );
            }

            // (g) the structure, checked after the four comparisons: a
            // header byte made malformed and the page re-sealed, read
            // through a reference to the new crc, passes all four and is
            // refused by the structural checks.
            // (h) the order: the same byte damaged without re-sealing
            // fails the self CRC first, before the structure is looked at.
            for (at, v) in [(4, 0u8), (4, 10), (5, 1), (7, 0x80), (22, 1)] {
                let mut g = pg.clone();
                g[at] = v;
                let mut sealed = g.clone();
                let g_crc = page::seal(&mut sealed).unwrap();
                let rg = Ref { crc: g_crc, ..r };
                assert_eq!(
                    four_checks(&sealed, &rg, ID),
                    [true; 4],
                    "{ctx}: (g) fixture"
                );
                assert_eq!(oracle_header(&sealed), None, "{ctx}: (g) byte {at} = {v}");
                expect_any_corrupt(
                    page::verify(&sealed, &rg, ID),
                    &format!("{ctx}: (g) byte {at}"),
                );
                assert_eq!(
                    four_checks(&g, &r, ID),
                    [false, true, true, true],
                    "{ctx}: (h) fixture"
                );
                expect_corrupt(
                    page::verify(&g, &r, ID),
                    "CRC32C of its bytes",
                    &format!("{ctx}: (h) byte {at}"),
                );
            }
        }
    }
    for len in BAD_LENS {
        let r = Ref {
            slot: 77,
            epoch: 11,
            crc: 0,
        };
        expect_corrupt(
            page::verify(&vec![0u8; len], &r, 0),
            "not an allowed page size",
            &format!("(f) {len} bytes"),
        );
    }
}

/// §15.1 I-KEY-SIZE, A-2, full: the formula and its values (Work 3's
/// table), the four-cell guarantee on real pages, and the size check.
#[test]
fn row_i_key_size() {
    for [ps, u, q, k, v, leaf, inner] in LIMITS {
        let ctx = format!("{ps} bytes");
        // (a) the formula's values.
        assert_eq!(page::usable(ps), u, "{ctx}: U");
        assert_eq!(page::k_max(ps), k, "{ctx}: K_MAX");
        assert_eq!(page::v_inline(ps), v, "{ctx}: V_INLINE");
        assert_eq!(page::max_leaf_cost(ps), leaf, "{ctx}: max leaf cell");
        assert_eq!(page::max_inner_cost(ps), inner, "{ctx}: max inner cell");
        assert_eq!(leaf, q, "{ctx}: a maximum leaf cell costs Q");
        assert_eq!(4 * q, u, "{ctx}: four maximum cells fill U exactly");
        assert_eq!(
            u / inner,
            11,
            "{ctx}: an inner page holds eleven separators"
        );

        // (b) four maximum leaf cells fill an `init`ed leaf exactly.
        let mut lf = vec![0u8; ps];
        page::init(&mut lf, PageKind::Leaf).unwrap();
        let val = vec![0x5Au8; v];
        for i in 0..4u32 {
            // A leaf with room for exactly one more maximum cell is safe.
            assert!(page::is_safe(&lf).unwrap(), "{ctx}: {i} cells, not safe");
            let key = ckey(i, k);
            let pos = page::leaf_find(&lf, &key).unwrap().unwrap_err();
            assert!(
                page::leaf_insert(&mut lf, pos, &key, &val).unwrap(),
                "{ctx}: maximum leaf cell {i} refused"
            );
        }
        assert_eq!(page::free_bytes(&lf).unwrap(), 0, "{ctx}: leaf free bytes");
        assert!(
            !page::is_safe(&lf).unwrap(),
            "{ctx}: a full leaf is not safe"
        );
        assert!(
            !page::leaf_insert(&mut lf, 4, &ckey(4, k), &val).unwrap(),
            "{ctx}: a fifth maximum cell fits"
        );
        // ... and four (indeed eleven) maximum separators an inner page.
        let mut ip = vec![0u8; ps];
        page::init(&mut ip, PageKind::Inner).unwrap();
        for i in 0..11u32 {
            assert!(
                page::inner_insert(&mut ip, i as usize, PageId(u64::from(i) + 1), &ckey(i, k))
                    .unwrap(),
                "{ctx}: maximum separator {i} refused"
            );
        }
        assert!(
            !page::inner_insert(&mut ip, 11, PageId(12), &ckey(11, k)).unwrap(),
            "{ctx}: a twelfth maximum separator fits"
        );

        // (c) the size check: `|k| + zeros(k) + 10 ≤ K_MAX` (§7.2).
        assert_eq!(
            page::check_key_size(ps, &vec![0x41; k - 10]).unwrap(),
            k,
            "{ctx}"
        );
        match page::check_key_size(ps, &vec![0x41; k - 9]) {
            Err(StoreError::KeyTooLarge(n)) => assert_eq!(n, k + 1, "{ctx}"),
            other => panic!("{ctx}: a key one byte over K_MAX gave {other:?}"),
        }
        let z = (k - 10) / 2;
        assert_eq!(
            page::check_key_size(ps, &vec![0; z]).unwrap(),
            2 * z + 10,
            "{ctx}"
        );
        match page::check_key_size(ps, &vec![0; z + 1]) {
            Err(StoreError::KeyTooLarge(n)) => assert_eq!(n, 2 * z + 12, "{ctx}"),
            other => panic!("{ctx}: {} zero bytes gave {other:?}", z + 1),
        }
        let mixed_key = [7u8, 0, 0, 9, 0, 1, 2, 0];
        assert_eq!(page::entry_key_len(&mixed_key), 8 + 4 + 10, "{ctx}");
        assert_eq!(page::check_key_size(ps, &mixed_key).unwrap(), 22, "{ctx}");
    }
    assert_eq!(page::entry_key_len(&[]), 10);
    // (d) the check at a size outside PAGE_SIZES.
    for len in BAD_LENS {
        expect_invalid(page::check_key_size(len, b"k"), &format!("(d) {len}"));
    }
}

// ---------------------------------------------------------------------------
// The header codec and the slotted body (card A-2's own tests)
// ---------------------------------------------------------------------------

/// A-0-F2: `header` refuses `free_start > free_end` and `free_end > len`
/// itself, and `free_bytes` and `is_safe` are `Corrupt` too (their checked
/// subtraction alone would be, without `header`'s check: HEADER-ORDER-OFF).
#[test]
fn header_order_corrupt() {
    let mut base = vec![0u8; 8192];
    page::init(&mut base, PageKind::Leaf).unwrap();
    page::header(&base).unwrap();

    let mut p = base.clone();
    p[28..32].copy_from_slice(&100u32.to_le_bytes());
    p[32..36].copy_from_slice(&90u32.to_le_bytes());
    expect_any_corrupt(page::header(&p), "free_start 100 > free_end 90: header");
    expect_any_corrupt(
        page::free_bytes(&p),
        "free_start 100 > free_end 90: free_bytes",
    );
    expect_any_corrupt(page::is_safe(&p), "free_start 100 > free_end 90: is_safe");

    let mut q = base.clone();
    q[32..36].copy_from_slice(&8193u32.to_le_bytes());
    expect_any_corrupt(page::header(&q), "free_end 8193: header");
    expect_any_corrupt(page::free_bytes(&q), "free_end 8193: free_bytes");
    expect_any_corrupt(page::is_safe(&q), "free_end 8193: is_safe");
}

/// §3.1's 24 header bytes: `write_header`, `seal` and `read_header`
/// round-trip for every kind at every size, each byte where the spec puts
/// it; every malformed header is `Corrupt`; Work 1's golden vectors.
#[test]
fn page_header_codec() {
    for b in 0..=255u8 {
        let kind = Kind::from_byte(b);
        assert_eq!(kind.is_some(), (3..=9).contains(&b), "kind byte {b}");
        if let Some(k) = kind {
            assert_eq!(k as u8, b, "kind byte {b}");
        }
    }
    for ps in SIZES {
        for kb in 3u8..=9 {
            let kind = Kind::from_byte(kb).unwrap();
            let id = 0xF0E1_D2C3_B4A5_9687 ^ u64::from(kb);
            let epoch = 0x8000_0001 ^ u32::from(kb);
            let ctx = format!("{ps} bytes, kind {kb}");
            let mut pg = patterned(ps);
            page::write_header(&mut pg, kind, id, epoch).unwrap();
            assert_eq!(pg[..4], patterned(ps)[..4], "{ctx}: write_header left crc");
            assert_eq!(pg[4], kb, "{ctx}: kind @4");
            assert_eq!(pg[5], 0, "{ctx}: flags @5");
            assert_eq!(pg[6..8], [0, 0], "{ctx}: reserved @6");
            assert_eq!(pg[8..16], id.to_le_bytes(), "{ctx}: id @8");
            assert_eq!(pg[16..20], epoch.to_le_bytes(), "{ctx}: epoch @16");
            assert_eq!(pg[20..24], [0; 4], "{ctx}: reserved @20");
            assert_eq!(pg[24..], patterned(ps)[24..], "{ctx}: body untouched");
            let crc = page::seal(&mut pg).unwrap();
            assert_eq!(crc, crc32c_bitwise(&pg[4..]), "{ctx}: seal's crc");
            assert_eq!(pg[..4], crc.to_le_bytes(), "{ctx}: crc @0");
            assert_eq!(
                page::read_header(&pg).unwrap(),
                PageHeader {
                    crc,
                    kind,
                    id,
                    epoch
                },
                "{ctx}: round trip"
            );
            assert_eq!(
                oracle_header(&pg),
                Some((crc, kb, id, epoch)),
                "{ctx}: oracle"
            );
        }

        let mut good = patterned(ps);
        page::write_header(&mut good, Kind::Leaf, 7, 3).unwrap();
        page::seal(&mut good).unwrap();
        page::read_header(&good).unwrap();
        let mut bad_pages: Vec<(String, Vec<u8>)> = Vec::new();
        for k in [0u8, 1, 2, 10, 255] {
            let mut p = good.clone();
            p[4] = k;
            bad_pages.push((format!("kind {k}"), p));
        }
        for (at, v) in [(5, 1u8), (5, 0x80), (6, 1), (7, 0x80)] {
            let mut p = good.clone();
            p[at] = v;
            bad_pages.push((format!("byte {at} = {v:#x}"), p));
        }
        for at in 20..24 {
            let mut p = good.clone();
            p[at] = 0x10;
            bad_pages.push((format!("byte {at}"), p));
        }
        for (what, mut p) in bad_pages {
            // Re-sealed, so only the structure can refuse it.
            page::seal(&mut p).unwrap();
            assert_eq!(oracle_header(&p), None, "{ps} bytes, {what}: oracle");
            expect_any_corrupt(page::read_header(&p), &format!("{ps} bytes, {what}"));
        }
        let mut grown = good.clone();
        grown.push(0);
        expect_any_corrupt(
            page::read_header(&good[..ps - 1]),
            &format!("{ps} bytes cut"),
        );
        expect_any_corrupt(page::read_header(&grown), &format!("{ps} bytes grown"));
    }
    for len in BAD_LENS {
        let mut p = vec![0u8; len];
        if len > 4 {
            p[4] = 3;
        }
        expect_any_corrupt(page::read_header(&p), &format!("{len} bytes"));
        let before = p.clone();
        expect_invalid(
            page::write_header(&mut p, Kind::Leaf, 1, 1),
            &format!("{len}"),
        );
        expect_invalid(page::seal(&mut p), &format!("seal {len}"));
        assert_eq!(p, before, "{len} bytes: a refused write changed the buffer");
    }

    // Work 1's golden vectors: `init(Leaf)`, `write_header(Leaf, 7, 3)`,
    // `seal`, on a zeroed page.
    let golden = [
        (
            4096,
            0x64EB_FE3B_u32,
            "3bfeeb640300000007000000000000000300000000000000000000002c000000001000000000000000000000",
        ),
        (
            65536,
            0x3349_45A7,
            "a74549330300000007000000000000000300000000000000000000002c000000000001000000000000000000",
        ),
    ];
    for (ps, crc, first44) in golden {
        let mut pg = vec![0u8; ps];
        page::init(&mut pg, PageKind::Leaf).unwrap();
        page::write_header(&mut pg, Kind::Leaf, 7, 3).unwrap();
        assert_eq!(page::seal(&mut pg).unwrap(), crc, "{ps}: golden crc");
        assert_eq!(hex(&pg[..44]), first44, "{ps}: golden bytes");
        assert!(pg[44..].iter().all(|&b| b == 0), "{ps}: golden tail");
    }
}

/// The slotted body on the new header at every allowed size (§3.1: "every
/// in-page offset field must address the whole page at every allowed page
/// size"): an empty page's pointers, a fill to refusal with cells of mixed
/// sizes, a removal of every cell and a compaction, and P-SPLIT's promise
/// that both halves of a split accept one more maximum cell, for leaves
/// and for inner pages. At 65536 bytes `free_end` is 65536, and a corrupt
/// empty cell whose compacted offset would be 65536 is refused.
#[test]
fn slotted_every_page_size() {
    for ps in SIZES {
        let (kmax, vmax) = (page::k_max(ps), page::v_inline(ps));
        let ctx = format!("{ps} bytes");

        // The empty pages `init` writes, byte for byte, over any old bytes;
        // and what `header` refuses in the B+tree header.
        for (kind, kb) in [(PageKind::Leaf, 3u8), (PageKind::Inner, 4)] {
            let mut e = patterned(ps);
            page::init(&mut e, kind).unwrap();
            assert_eq!(e[..44], init_bytes(ps, kb), "{ctx}: init({kind:?})");
            assert_eq!(e[44..], patterned(ps)[44..], "{ctx}: init({kind:?}) body");
            let mut bad: Vec<(String, Vec<u8>)> = Vec::new();
            for at in [26, 27] {
                let mut p = e.clone();
                p[at] = 1;
                bad.push((format!("byte {at}"), p));
            }
            let mut p = e.clone();
            p[24..26].copy_from_slice(&1u16.to_le_bytes());
            bad.push(("nslots 1 with free_start 44".into(), p));
            // The high bytes of free_start and free_end: free_start at or
            // above 2^16 (bytes 30, 31) and free_end past the page (bytes 34,
            // 35), so each u32 is read whole.
            for (at, v) in [(30, 1u8), (31, 1), (34, 2), (35, 1)] {
                let mut p = e.clone();
                p[at] = v;
                bad.push((format!("byte {at} = {v}"), p));
            }
            for k in [5u8, 6, 7, 8, 9] {
                let mut p = e.clone();
                p[4] = k;
                bad.push((format!("kind {k}"), p));
            }
            for (what, p) in bad {
                assert_eq!(oracle_slotted(&p), None, "{ctx}: {kind:?}, {what}: oracle");
                expect_any_corrupt(page::header(&p), &format!("{ctx}: {kind:?}, {what}"));
            }
        }

        // --- Leaf ---
        let mut lf = vec![0u8; ps];
        page::init(&mut lf, PageKind::Leaf).unwrap();
        let h = page::header(&lf).unwrap();
        assert_eq!(
            (h.kind, h.nslots, h.free_start, h.free_end),
            (PageKind::Leaf, 0, 44, ps),
            "{ctx}"
        );
        assert_eq!(
            oracle_slotted(&lf),
            Some((3, 0, 44, ps)),
            "{ctx}: raw B+tree header"
        );
        assert_eq!(
            le_u32(&lf, 32) as usize,
            ps,
            "{ctx}: free_end @32 is the page size"
        );
        let cell = |i: u32| {
            (
                ckey(2 * i, mixed(i, 4, kmax)),
                vec![(i % 251) as u8; mixed(i + 1, 0, vmax)],
            )
        };
        let mut n = 0u32;
        let pending = loop {
            let (k, v) = cell(n);
            let pos = page::leaf_find(&lf, &k).unwrap().unwrap_err();
            assert_eq!(pos, n as usize, "{ctx}: ascending keys append");
            if !page::leaf_insert(&mut lf, pos, &k, &v).unwrap() {
                break (k, v);
            }
            n += 1;
        };
        assert!(
            n >= 4,
            "{ctx}: a leaf took only {n} cells of at most maximum size"
        );
        for i in 0..n {
            let (k, v) = cell(i);
            assert_eq!(
                page::leaf_cell(&lf, i as usize).unwrap(),
                (k.as_slice(), v.as_slice()),
                "{ctx}: cell {i}"
            );
        }
        // Remove every cell, then compact: the page is empty again.
        let mut emptied = lf.clone();
        for i in 0..n {
            page::leaf_remove(&mut emptied, ((i * 7) % (n - i)) as usize).unwrap();
        }
        page::compact(&mut emptied).unwrap();
        let h = page::header(&emptied).unwrap();
        assert_eq!(
            (h.nslots, h.free_start, h.free_end),
            (0, 44, ps),
            "{ctx}: emptied leaf"
        );
        assert_eq!(
            page::hole_bytes(&emptied).unwrap(),
            0,
            "{ctx}: emptied leaf holes"
        );
        // Split the full leaf with the pending cell (P-SPLIT).
        let (mut left, mut right) = (lf.clone(), vec![0u8; ps]);
        let sep = page::leaf_split(&mut left, &mut right, (&pending.0, &pending.1)).unwrap();
        let (hl, hr) = (page::header(&left).unwrap(), page::header(&right).unwrap());
        assert_eq!(
            hl.nslots + hr.nslots,
            n as usize + 1,
            "{ctx}: split keeps every cell"
        );
        assert!(
            hl.nslots > 0 && hr.nslots > 0,
            "{ctx}: both halves non-empty"
        );
        assert_eq!(
            page::leaf_cell(&right, 0).unwrap().0,
            sep.as_slice(),
            "{ctx}: separator"
        );
        let max_val = vec![0xC3u8; vmax];
        for (half, key) in [
            (&mut left, ckey(1, kmax)),
            (&mut right, ckey(2 * n + 1, kmax)),
        ] {
            let pos = page::leaf_find(half, &key).unwrap().unwrap_err();
            assert!(
                page::leaf_insert(half, pos, &key, &max_val).unwrap(),
                "{ctx}: a half of a leaf split refused a maximum cell"
            );
        }

        // --- Inner ---
        let mut ip = vec![0u8; ps];
        page::init(&mut ip, PageKind::Inner).unwrap();
        let h = page::header(&ip).unwrap();
        assert_eq!(
            (h.kind, h.nslots, h.free_start, h.free_end),
            (PageKind::Inner, 0, 44, ps),
            "{ctx}"
        );
        assert_eq!(
            oracle_slotted(&ip),
            Some((4, 0, 44, ps)),
            "{ctx}: raw inner header"
        );
        let sep_of = |i: u32| ckey(2 * i, mixed(i, 4, kmax));
        // Child ids whose top six bytes are all non-zero (the low two count
        // the cells), so a cell's u64 child is read whole.
        let child_of = |i: u32| PageId(0xA1B2_C3D4_E5F6_0000 + u64::from(i));
        let mut m = 0u32;
        let pending = loop {
            let s = sep_of(m);
            let pos = page::inner_cell_position(&ip, &s).unwrap().unwrap_err();
            if !page::inner_insert(&mut ip, pos, child_of(m), &s).unwrap() {
                break (child_of(m), s);
            }
            m += 1;
        };
        // A right-most child with every byte non-zero, so the u64 at 36
        // is read whole.
        let rc = PageId(0xF1E2_D3C4_B5A6_9788);
        page::set_right_child(&mut ip, rc).unwrap();
        assert_eq!(le_u64(&ip, 36), rc.0, "{ctx}: right child @36");
        assert_eq!(page::right_child(&ip).unwrap(), rc, "{ctx}: right child");
        assert!(m >= 4, "{ctx}: an inner page took only {m} separators");
        for i in 0..m {
            assert_eq!(
                page::inner_cell(&ip, i as usize).unwrap(),
                (child_of(i), sep_of(i).as_slice()),
                "{ctx}: inner cell {i}"
            );
        }
        let mut emptied = ip.clone();
        for _ in 0..m {
            page::inner_remove(&mut emptied, 0).unwrap();
        }
        page::compact(&mut emptied).unwrap();
        let h = page::header(&emptied).unwrap();
        assert_eq!(
            (h.nslots, h.free_start, h.free_end),
            (0, 44, ps),
            "{ctx}: emptied inner"
        );
        let (mut left, mut right) = (ip.clone(), vec![0u8; ps]);
        let up = page::inner_split(&mut left, &mut right, (pending.0, &pending.1)).unwrap();
        let (hl, hr) = (page::header(&left).unwrap(), page::header(&right).unwrap());
        assert_eq!(
            hl.nslots + hr.nslots + 1,
            m as usize + 1,
            "{ctx}: inner split keeps every cell"
        );
        assert!(
            hl.nslots > 0 && hr.nslots > 0,
            "{ctx}: both inner halves non-empty"
        );
        assert_eq!(
            page::right_child(&right).unwrap(),
            rc,
            "{ctx}: right child moves right"
        );
        assert_eq!(
            page::inner_route(&left, &up).unwrap().0,
            hl.nslots,
            "{ctx}: up routes past left"
        );
        for (half, s) in [
            (&mut left, ckey(1, kmax)),
            (&mut right, ckey(2 * m + 1, kmax)),
        ] {
            let pos = page::inner_cell_position(half, &s).unwrap().unwrap_err();
            assert!(
                page::inner_insert(half, pos, PageId(7), &s).unwrap(),
                "{ctx}: a half of an inner split refused a maximum separator"
            );
        }
    }

    // A corrupt empty cell at the end of a 65536-byte page: its compacted
    // offset would be 65536, which a u16 slot cannot hold. `compact`
    // refuses it and leaves the page as it was.
    let mut p = vec![0u8; 65536];
    page::init(&mut p, PageKind::Leaf).unwrap();
    p[24..26].copy_from_slice(&1u16.to_le_bytes());
    p[28..32].copy_from_slice(&48u32.to_le_bytes());
    p[32..36].copy_from_slice(&65535u32.to_le_bytes());
    p[44..46].copy_from_slice(&65535u16.to_le_bytes());
    p[46..48].copy_from_slice(&0u16.to_le_bytes());
    assert_eq!(
        page::slot(&p, 0).unwrap(),
        (65535, 0),
        "the empty cell is in bounds"
    );
    let before = p.clone();
    expect_any_corrupt(page::compact(&mut p), "compact of an empty cell at 65536");
    assert_eq!(p, before, "a refused compaction changed the page");
}

// ---------------------------------------------------------------------------
// map.rs: positions, geometry, map pages
// ---------------------------------------------------------------------------

/// §3.1's positions, card A-2's encoding `radix << 63 | level << 56 |
/// index`: the card's values, the bounds, and a sample in which the two
/// radixes never share an id and `position_of` inverts both.
#[test]
fn position_ids() {
    assert_eq!(map::position_id(Radix::Map, 0, 5).unwrap(), 5);
    assert_eq!(map::position_id(Radix::Map, 1, 0).unwrap(), 1 << 56);
    assert_eq!(map::position_id(Radix::FreeSet, 0, 0).unwrap(), 1 << 63);
    let top = map::position_id(Radix::Map, 127, (1 << 56) - 1).unwrap();
    assert_eq!(top, (127 << 56) | ((1 << 56) - 1));
    assert_eq!(map::position_of(top), (Radix::Map, 127, (1 << 56) - 1));
    expect_invalid(map::position_id(Radix::Map, 128, 0), "level 128");
    expect_invalid(map::position_id(Radix::FreeSet, 255, 0), "level 255");
    expect_invalid(map::position_id(Radix::Map, 0, 1 << 56), "index 2^56");
    expect_invalid(
        map::position_id(Radix::FreeSet, 0, u64::MAX),
        "index u64::MAX",
    );
    let mut seen = std::collections::HashSet::new();
    for level in [0u8, 1, 2, 63, 126, 127] {
        for index in [0u64, 1, 509, 510, 1 << 32, (1 << 56) - 1] {
            let m = map::position_id(Radix::Map, level, index).unwrap();
            let f = map::position_id(Radix::FreeSet, level, index).unwrap();
            assert_eq!(
                m,
                (u64::from(level) << 56) | index,
                "map ({level}, {index})"
            );
            assert_eq!(
                f,
                (1 << 63) | (u64::from(level) << 56) | index,
                "free set ({level}, {index})"
            );
            assert_ne!(m, f);
            assert_eq!(map::position_of(m), (Radix::Map, level, index));
            assert_eq!(map::position_of(f), (Radix::FreeSet, level, index));
            assert!(
                seen.insert(m) && seen.insert(f),
                "({level}, {index}) shares an id"
            );
        }
    }
    for id in [
        0u64,
        1,
        u64::MAX,
        1 << 63,
        (1 << 63) - 1,
        0x0123_4567_89AB_CDEF,
        0xFEDC_BA98_7654_3210,
    ] {
        let (r, l, i) = map::position_of(id);
        assert_eq!(
            map::position_id(r, l, i).unwrap(),
            id,
            "position_of({id:#x}) is total"
        );
    }
}

/// §4.1's geometry: the fanout at each size, a test fanout's bounds, the
/// LPID entry, child and parent, the root level and a cover's overflow.
#[test]
fn map_geometry() {
    for (ps, f) in SIZES.into_iter().zip([254u64, 510, 1022, 2046, 4094]) {
        assert_eq!(map::max_fanout(ps), f, "{ps}");
        let fu = f as usize;
        assert!(
            24 + 16 * fu <= ps && 24 + 16 * (fu + 1) > ps,
            "{ps}: the most refs that fit"
        );
        assert_eq!(MapGeometry::new(ps).unwrap().fanout(), f, "{ps}");
        assert_eq!(MapGeometry::with_fanout(ps, f).unwrap().fanout(), f, "{ps}");
        expect_invalid(MapGeometry::with_fanout(ps, f + 1), &format!("{ps}: f + 1"));
    }
    expect_invalid(MapGeometry::with_fanout(4096, 0), "f = 0");
    expect_invalid(MapGeometry::with_fanout(4096, 1), "f = 1");
    expect_invalid(MapGeometry::with_fanout(4096, 255), "f = 255");
    assert_eq!(MapGeometry::with_fanout(4096, 2).unwrap().fanout(), 2);
    for len in BAD_LENS {
        expect_invalid(MapGeometry::new(len), &format!("new({len})"));
        expect_invalid(
            MapGeometry::with_fanout(len, 2),
            &format!("with_fanout({len}, 2)"),
        );
    }

    let g = MapGeometry::new(8192).unwrap();
    assert_eq!(g.fanout(), 510);
    assert_eq!(g.lpid_entry(0), (0, 0));
    assert_eq!(g.lpid_entry(509), (0, 509));
    assert_eq!(g.lpid_entry(510), (1, 0));
    assert_eq!(
        g.lpid_entry(u64::MAX),
        (u64::MAX / 510, (u64::MAX % 510) as usize)
    );
    for (level, index, slot) in [
        (1u8, 0u64, 0u64),
        (1, 0, 509),
        (2, 3, 17),
        (5, 1000, 509),
        (127, 7, 1),
    ] {
        let (cl, ci) = g.child(level, index, slot).unwrap();
        assert_eq!(
            (cl, ci),
            (level - 1, index * 510 + slot),
            "child of ({level}, {index})"
        );
        assert_eq!(
            g.parent(cl, ci),
            (level, index, slot),
            "parent of ({cl}, {ci})"
        );
    }
    expect_invalid(g.child(0, 0, 0), "a MapLeaf's child");
    expect_invalid(g.child(1, 0, 510), "slot f");
    expect_invalid(g.child(1, u64::MAX, 0), "index overflow");
    assert_eq!(g.root_level_for(0), 1);
    assert_eq!(g.root_level_for(509), 1);
    assert_eq!(g.root_level_for(510), 2);
    assert_eq!(g.root_level_for(510 * 510 - 1), 2);
    assert_eq!(g.root_level_for(510 * 510), 3);
    assert_eq!(g.leaf_cover(0), Some(1));
    assert_eq!(g.leaf_cover(1), Some(510));
    assert_eq!(g.leaf_cover(2), Some(510 * 510));

    let t = MapGeometry::with_fanout(4096, 2).unwrap();
    let levels: Vec<u8> = [1u64, 2, 3, 4]
        .iter()
        .map(|&i| t.root_level_for(i))
        .collect();
    assert_eq!(levels, [1, 2, 2, 3]);
    assert_eq!(t.root_level_for(0), 1);
    assert_eq!(t.root_level_for(u64::MAX), 64);
    assert_eq!(t.leaf_cover(63), Some(1 << 63));
    assert_eq!(t.leaf_cover(64), None);
    assert_eq!(t.lpid_entry(5), (2, 1));
}

/// §4.1's map page body at every size, for a MapLeaf and a MapInner, at the
/// full fanout and a test fanout of 3: the refs round-trip at `24 + 16·i`,
/// the sealed page passes `verify` and `check_map_page`, every refusal is
/// the right error, and Work 4's golden vectors.
#[test]
fn map_page_codec() {
    let first = Ref {
        slot: 300,
        epoch: 8,
        crc: 0xCAFE_BABE,
    };
    let last = Ref {
        slot: 301,
        epoch: 9,
        crc: 0x0102_0304,
    };
    for ps in SIZES {
        for geom in [
            MapGeometry::new(ps).unwrap(),
            MapGeometry::with_fanout(ps, 3).unwrap(),
        ] {
            let f = geom.fanout();
            let fu = f as usize;
            for (level, index) in [(0u8, 2u64), (1, 0)] {
                let ctx = format!("{ps} bytes, f {f}, level {level}");
                let id = (u64::from(level) << 56) | index;
                let mut pg = vec![0xEEu8; ps];
                map::init_map_page(&mut pg, &geom, level, index, 9).unwrap();
                assert!(
                    pg[..4].iter().chain(&pg[24..]).all(|&b| b == 0),
                    "{ctx}: init zeroes"
                );
                assert_eq!(
                    oracle_header(&pg),
                    Some((0, if level == 0 { 6 } else { 7 }, id, 9)),
                    "{ctx}"
                );
                map::set_map_ref(&mut pg, &geom, 0, first).unwrap();
                map::set_map_ref(&mut pg, &geom, f - 1, last).unwrap();
                assert_eq!(pg[24..40], ref_bytes(first), "{ctx}: ref 0 @24");
                let at = 24 + 16 * (fu - 1);
                assert_eq!(pg[at..at + 16], ref_bytes(last), "{ctx}: ref f-1");
                let crc = page::seal(&mut pg).unwrap();
                let r = Ref {
                    slot: 1234,
                    epoch: 9,
                    crc,
                };
                let h = page::verify(&pg, &r, id).unwrap();
                let want_kind = if level == 0 {
                    Kind::MapLeaf
                } else {
                    Kind::MapInner
                };
                assert_eq!(h.kind, want_kind, "{ctx}");
                assert!(oracle_map_ok(&pg, fu, level, index), "{ctx}: oracle");
                map::check_map_page(&pg, &geom, level, index).unwrap();
                for i in 0..f {
                    let want = if i == 0 {
                        first
                    } else if i == f - 1 {
                        last
                    } else {
                        Ref::NULL
                    };
                    assert_eq!(map::map_ref(&pg, &geom, i).unwrap(), want, "{ctx}: ref {i}");
                }

                // Refusals of a caller's bug: Invalid, the page unchanged.
                let mut w = pg.clone();
                expect_invalid(
                    map::set_map_ref(&mut w, &geom, f, first),
                    &format!("{ctx}: ref f"),
                );
                let slot0 = Ref {
                    slot: 0,
                    epoch: 1,
                    crc: 0,
                };
                expect_invalid(
                    map::set_map_ref(&mut w, &geom, 1, slot0),
                    &format!("{ctx}: slot 0, epoch 1"),
                );
                let slot0 = Ref {
                    slot: 0,
                    epoch: 0,
                    crc: 5,
                };
                expect_invalid(
                    map::set_map_ref(&mut w, &geom, 1, slot0),
                    &format!("{ctx}: slot 0, crc 5"),
                );
                assert_eq!(w, pg, "{ctx}: a refused set changed the page");
                expect_invalid(map::map_ref(&pg, &geom, f), &format!("{ctx}: map_ref f"));

                // Refusals of damaged bytes: Corrupt (each re-sealed, so
                // only the body check can refuse it).
                let mut bad: Vec<(&str, Vec<u8>)> = Vec::new();
                let mut p = pg.clone();
                p[4] = if level == 0 { 7 } else { 6 };
                bad.push(("the other map kind", p));
                let mut p = pg.clone();
                p[8..16].copy_from_slice(&(id + 1).to_le_bytes());
                bad.push(("the id of index + 1", p));
                let mut p = pg.clone();
                p[8..16].copy_from_slice(&((1 << 63) | id).to_le_bytes());
                bad.push(("the free set's id", p));
                // Every bit of the header id, so that the whole id (radix,
                // level and index) is compared.
                for bit in 0..64 {
                    let mut p = pg.clone();
                    p[8..16].copy_from_slice(&(id ^ (1u64 << bit)).to_le_bytes());
                    bad.push(("an id one bit off the position's", p));
                }
                // The first and the last ref with slot 0 but their epoch
                // and crc kept, so that every ref is checked.
                for (i, what) in [
                    (0, "ref 0 with slot 0 and a non-zero epoch and crc"),
                    (fu - 1, "ref f-1 with slot 0 and a non-zero epoch and crc"),
                ] {
                    let mut p = pg.clone();
                    p[24 + 16 * i..24 + 16 * i + 8].fill(0);
                    bad.push((what, p));
                }
                let mut p = pg.clone();
                p[24 + 16 + 12] = 1;
                bad.push(("an unmapped entry with a non-zero crc", p));
                let mut p = pg.clone();
                p[24 + 16 + 8] = 1;
                bad.push(("an unmapped entry with a non-zero epoch", p));
                let mut p = pg.clone();
                p[ps - 1] = 1;
                bad.push(("the last byte", p));
                let mut p = pg.clone();
                p[24 + 16 * fu] = 1;
                bad.push(("the first byte past the refs", p));
                if f == 3 {
                    let mut p = pg.clone();
                    p[24 + 16 * 3..24 + 16 * 4].copy_from_slice(&ref_bytes(first));
                    bad.push(("a ref at index f", p));
                }
                for (what, mut p) in bad {
                    page::seal(&mut p).unwrap();
                    assert!(
                        !oracle_map_ok(&p, fu, level, index),
                        "{ctx}, {what}: oracle"
                    );
                    expect_any_corrupt(
                        map::check_map_page(&p, &geom, level, index),
                        &format!("{ctx}: {what}"),
                    );
                }
                expect_any_corrupt(
                    map::check_map_page(&pg[..ps - 1], &geom, level, index),
                    &format!("{ctx}: cut"),
                );
                expect_invalid(
                    map::check_map_page(&pg, &geom, 128, 0),
                    &format!("{ctx}: level 128"),
                );
            }
        }
    }
    // A test fanout's page is no full-fanout page, and the reverse.
    let mut p3 = vec![0u8; 4096];
    let g3 = MapGeometry::with_fanout(4096, 3).unwrap();
    map::init_map_page(&mut p3, &g3, 0, 0, 1).unwrap();
    map::set_map_ref(&mut p3, &MapGeometry::new(4096).unwrap(), 200, first).unwrap();
    expect_any_corrupt(
        map::check_map_page(&p3, &g3, 0, 0),
        "a full-fanout ref on a test-fanout page",
    );

    // Work 4's golden vectors, at 4096 bytes and f = 254.
    let g = MapGeometry::new(4096).unwrap();
    let mut leaf = vec![0u8; 4096];
    map::init_map_page(&mut leaf, &g, 0, 2, 9).unwrap();
    map::set_map_ref(&mut leaf, &g, 0, first).unwrap();
    map::set_map_ref(&mut leaf, &g, 253, last).unwrap();
    assert_eq!(
        page::seal(&mut leaf).unwrap(),
        0x5885_22FF,
        "golden MapLeaf"
    );
    let mut inner = vec![0u8; 4096];
    map::init_map_page(&mut inner, &g, 1, 0, 9).unwrap();
    assert_eq!(page::read_header(&inner).unwrap().id, 0x0100_0000_0000_0000);
    map::set_map_ref(&mut inner, &g, 0, first).unwrap();
    assert_eq!(
        page::seal(&mut inner).unwrap(),
        0xB60D_78CB,
        "golden MapInner"
    );
}

// ---------------------------------------------------------------------------
// Fuzz targets (PLAN-v3 §6.2: "the page decoder, page-map decoder") and
// their proptest drivers. None may panic; each returns `Err` with the
// property it found broken.
// ---------------------------------------------------------------------------

/// The page-header decoder: `read_header` on `data` cut or zero-padded to
/// 4096 bytes agrees with the raw-byte oracle, and a header it accepts
/// writes back to the same bytes 4..24 (canonical).
fn fuzz_header(data: &[u8]) -> Result<(), String> {
    let page = fit(data, 4096);
    let got = page::read_header(&page);
    match (&got, oracle_header(&page)) {
        (Ok(h), Some((crc, kb, id, epoch))) => {
            if (h.crc, h.kind as u8, h.id, h.epoch) != (crc, kb, id, epoch) {
                return Err(format!(
                    "read_header gave {h:?}, the bytes say {:?}",
                    (crc, kb, id, epoch)
                ));
            }
            let mut z = vec![0u8; 4096];
            page::write_header(&mut z, h.kind, h.id, h.epoch).map_err(|e| e.to_string())?;
            if z[4..24] != page[4..24] {
                return Err("a decoded header does not write back to its bytes".into());
            }
            Ok(())
        }
        (Err(StoreError::Corrupt(_)), None) => Ok(()),
        (got, want) => Err(format!("read_header gave {got:?}, the oracle {want:?}")),
    }
}

/// I-INTEGRITY's check: `verify` of the bytes before `data`'s last 24,
/// with the reference (`slot`, `epoch`, `crc`) and the expected id taken
/// from those last 24 bytes (little-endian; zero-padded when `data` is
/// shorter). It returns `Ok` exactly when the length is an allowed size,
/// the four comparisons hold and the header decodes, by the oracles.
fn fuzz_verify(data: &[u8]) -> Result<(), String> {
    let (page, tail) = data.split_at(data.len().saturating_sub(24));
    let tail = fit(tail, 24);
    let r = Ref {
        slot: le_u64(&tail, 0),
        epoch: le_u32(&tail, 8),
        crc: le_u32(&tail, 12),
    };
    let id = le_u64(&tail, 16);
    let want_ok = SIZES.contains(&page.len())
        && four_checks(page, &r, id) == [true; 4]
        && oracle_header(page).is_some();
    match page::verify(page, &r, id) {
        Ok(h) if want_ok => {
            let want = oracle_header(page).ok_or("no oracle header")?;
            if (h.crc, h.kind as u8, h.id, h.epoch) == want {
                Ok(())
            } else {
                Err(format!("verify gave {h:?}, the bytes say {want:?}"))
            }
        }
        Ok(h) => Err(format!("verify accepted a page the oracle refuses: {h:?}")),
        Err(StoreError::Corrupt(m)) if want_ok => Err(format!("verify refused a good page: {m}")),
        Err(StoreError::Corrupt(_)) => Ok(()),
        Err(e) => Err(format!("verify gave a non-Corrupt error: {e:?}")),
    }
}

/// The page-map decoder: on `data` cut or zero-padded to 4096 bytes, at
/// `f` = 254 and at a test fanout of 3, `map_ref` reads every index as the
/// raw bytes say, and `check_map_page` agrees with the oracle at the
/// position the header names and at two fixed ones.
fn fuzz_map(data: &[u8]) -> Result<(), String> {
    let page = fit(data, 4096);
    for f in [254u64, 3] {
        let geom = MapGeometry::with_fanout(4096, f).map_err(|e| e.to_string())?;
        for i in 0..f {
            let at = 24 + 16 * i as usize;
            let want = Ref {
                slot: le_u64(&page, at),
                epoch: le_u32(&page, at + 8),
                crc: le_u32(&page, at + 12),
            };
            match map::map_ref(&page, &geom, i) {
                Ok(r) if r == want => {}
                other => {
                    return Err(format!(
                        "map_ref {i} at f {f}: {other:?}, bytes say {want:?}"
                    ));
                }
            }
        }
        let (_, level, index) = map::position_of(le_u64(&page, 8));
        for (l, ix) in [(level, index), (0, 0), (1, 0)] {
            let want = oracle_map_ok(&page, f as usize, l, ix);
            match (map::check_map_page(&page, &geom, l, ix), want) {
                (Ok(()), true) | (Err(StoreError::Corrupt(_)), false) => {}
                (got, want) => {
                    return Err(format!(
                        "check_map_page ({l}, {ix:#x}) at f {f}: {got:?}, oracle ok = {want}"
                    ));
                }
            }
        }
    }
    Ok(())
}

/// The slotted-body decoder: on `data` cut or zero-padded to 4096 bytes,
/// `header` agrees with the raw-byte oracle, and `slot`, `leaf_cell` and
/// `inner_cell` for every slot `header` admits, `free_bytes`,
/// `hole_bytes`, `is_safe`, `leaf_find`, `inner_route` and `compact` on a
/// copy each return `Ok` or `Corrupt`.
fn fuzz_slotted(data: &[u8]) -> Result<(), String> {
    let page = fit(data, 4096);
    fn ok_or_corrupt<T: std::fmt::Debug>(
        what: &str,
        r: Result<T, StoreError>,
    ) -> Result<(), String> {
        match r {
            Ok(_) | Err(StoreError::Corrupt(_)) => Ok(()),
            Err(e) => Err(format!("{what}: a non-Corrupt error {e:?}")),
        }
    }
    let h = page::header(&page);
    match (&h, oracle_slotted(&page)) {
        (Ok(h), Some((kb, nslots, fs, fe))) => {
            let kind = if kb == 3 {
                PageKind::Leaf
            } else {
                PageKind::Inner
            };
            if (h.kind, h.nslots, h.free_start, h.free_end) != (kind, nslots, fs, fe) {
                return Err(format!(
                    "header gave {h:?}, the bytes say {:?}",
                    (kb, nslots, fs, fe)
                ));
            }
            if page::free_bytes(&page).map_err(|e| e.to_string())? != fe - fs {
                return Err("free_bytes is not free_end - free_start".into());
            }
            for i in 0..h.nslots {
                ok_or_corrupt("slot", page::slot(&page, i))?;
                ok_or_corrupt("leaf_cell", page::leaf_cell(&page, i))?;
                ok_or_corrupt("inner_cell", page::inner_cell(&page, i))?;
            }
            if !matches!(page::slot(&page, h.nslots), Err(StoreError::Corrupt(_))) {
                return Err("the slot past nslots is not Corrupt".into());
            }
        }
        (Err(StoreError::Corrupt(_)), None) => {}
        (got, want) => return Err(format!("header gave {got:?}, the oracle {want:?}")),
    }
    ok_or_corrupt("free_bytes", page::free_bytes(&page))?;
    ok_or_corrupt("hole_bytes", page::hole_bytes(&page))?;
    ok_or_corrupt("is_safe", page::is_safe(&page))?;
    ok_or_corrupt("leaf_find", page::leaf_find(&page, &page[100..120]))?;
    ok_or_corrupt("inner_route", page::inner_route(&page, &page[200..230]))?;
    let mut copy = page.clone();
    ok_or_corrupt("compact", page::compact(&mut copy))
}

/// Every target on one page; `tail` is the 24 reference bytes `fuzz_verify`
/// reads after it.
fn run_all(page: &[u8], tail: &[u8]) -> Result<(), String> {
    fuzz_header(page)?;
    fuzz_map(page)?;
    fuzz_slotted(page)?;
    let mut with_ref = page.to_vec();
    with_ref.extend_from_slice(tail);
    fuzz_verify(&with_ref)
}

/// The 24 bytes `fuzz_verify` reads as a reference and an expected id.
fn ref_tail(r: Ref, id: u64) -> Vec<u8> {
    let mut v = ref_bytes(r);
    v.extend_from_slice(&id.to_le_bytes());
    v
}

/// The mutation corpus, built in code: a leaf and an inner page with cells,
/// a MapLeaf and a MapInner with refs, each with a header `id` and
/// `epoch`, sealed; with the reference and id that read it.
fn corpus(which: usize) -> Result<(Vec<u8>, Ref, u64), StoreError> {
    let mut p = vec![0u8; 4096];
    let (id, epoch) = match which {
        0 => {
            page::init(&mut p, PageKind::Leaf)?;
            for i in 0..6u32 {
                let k = ckey(2 * i, mixed(i, 4, 120));
                let v = vec![i as u8; mixed(i + 1, 0, 300)];
                let pos = page::leaf_find(&p, &k)?.unwrap_or_else(|at| at);
                if !page::leaf_insert(&mut p, pos, &k, &v)? {
                    return Err(StoreError::Invalid("corpus leaf overflow".into()));
                }
            }
            page::write_header(&mut p, Kind::Leaf, 42, 5)?;
            (42, 5)
        }
        1 => {
            page::init(&mut p, PageKind::Inner)?;
            for i in 0..5u32 {
                let s = ckey(2 * i, mixed(i, 4, 90));
                if !page::inner_insert(&mut p, i as usize, PageId(u64::from(i) + 50), &s)? {
                    return Err(StoreError::Invalid("corpus inner overflow".into()));
                }
            }
            page::set_right_child(&mut p, PageId(99))?;
            page::write_header(&mut p, Kind::Inner, 43, 6)?;
            (43, 6)
        }
        _ => {
            let level = if which == 2 { 0 } else { 1 };
            let g = MapGeometry::new(4096)?;
            map::init_map_page(&mut p, &g, level, 2, 7)?;
            for i in [0u64, 1, 100, 253] {
                let r = Ref {
                    slot: 1000 + i,
                    epoch: 7,
                    crc: 0xA5A5_0000 | i as u32,
                };
                map::set_map_ref(&mut p, &g, i, r)?;
            }
            (map::position_id(Radix::Map, level, 2)?, 7)
        }
    };
    let crc = page::seal(&mut p)?;
    Ok((
        p,
        Ref {
            slot: 3,
            epoch,
            crc,
        },
        id,
    ))
}

/// The drivers' configuration: 256 cases, and no regression file written
/// into the source tree on a failure (the case is in the failure message).
fn fuzz_config() -> ProptestConfig {
    ProptestConfig {
        cases: 256,
        failure_persistence: None,
        ..ProptestConfig::default()
    }
}

proptest! {
    #![proptest_config(fuzz_config())]

    /// Driver 1: a random 4096-byte page (shape 0), or a valid empty leaf
    /// (1) or inner page (2) whose first 64 bytes are random, each with a
    /// random 24-byte reference tail, into all four targets.
    #[test]
    fn page_fuzz_bytes(
        shape in 0u8..3,
        random in proptest::collection::vec(any::<u8>(), 4096),
        prefix in proptest::collection::vec(any::<u8>(), 64),
        tail in proptest::collection::vec(any::<u8>(), 24),
    ) {
        let pg = match shape {
            0 => random,
            s => {
                let mut p = vec![0u8; 4096];
                let kind = if s == 1 { PageKind::Leaf } else { PageKind::Inner };
                page::init(&mut p, kind).unwrap();
                p[..64].copy_from_slice(&prefix);
                p
            }
        };
        if let Err(m) = run_all(&pg, &tail) {
            prop_assert!(false, "{}", m);
        }
    }

    /// Driver 2: the corpus's valid pages, mutated by 1–8 random byte
    /// overwrites and re-sealed in about half the cases, so that `verify`
    /// reaches the structural checks, read through the page's reference.
    #[test]
    fn page_fuzz_mutated(
        which in 0usize..4,
        edits in proptest::collection::vec((0usize..4096, any::<u8>()), 1..=8),
        reseal in any::<bool>(),
    ) {
        let (mut p, mut r, id) = corpus(which).unwrap();
        for (at, b) in edits {
            p[at] = b;
        }
        if reseal {
            r.crc = page::seal(&mut p).unwrap();
        }
        if let Err(m) = run_all(&p, &ref_tail(r, id)) {
            prop_assert!(false, "{}", m);
        }
    }
}
