//! Page 0 and the meta record (card A-2-0, Work 3): their byte layouts,
//! encoders and validating decoders, the file's identity, the page
//! reference [`Ref`], the meta places, the replica region and its reserved
//! slots, and the boot-id serial order. S-STORE §2.1, §2.2, §2.4, §12.5.
//!
//! # Invariants this module owns
//!
//! - **The byte layouts** in the two tables below: §2.1's and §2.2's
//!   fields in the spec's order, packed, every integer little-endian, as
//!   card A-2-0 pins them under §2.2's "Card A-2 pins the bytes". Each
//!   record ends with the CRC32C of every byte above it, the magic and the
//!   format version included (§2.1, §2.2: "CRC32C of the bytes above"). The
//!   rest of the block is zero when written and never examined when read.
//! - **A malformed copy is invalid, never a panic** (§2.2). A decoder
//!   returns `StoreError::Corrupt` (XX001) for every byte sequence that is
//!   not a well-formed record, checking in this order: the block holds the
//!   whole record; the magic; the CRC; the field domains (`format_version`
//!   is [`FORMAT_VERSION`], `page_size` is in [`PAGE_SIZES`],
//!   `checksum_alg` is [`CHECKSUM_CRC32C`], `wal_seg_size` is not 0, and for
//!   a meta also its `copy` byte is 0 or 1, `map_level` is at least 1 and
//!   `epoch` is the low 32 bits of `ckpt_id`). That list is card A-2-0's pin
//!   of "a malformed copy". Every field is decoded at its own width, so a
//!   record that decodes encodes back to the same bytes.
//! - **§14 step 1's range check is not the decoder's.** A meta whose
//!   `T > ts_ckpt_hi` or `wal_start_lsn > wal_ckpt_end` decodes: the open
//!   checks the meta it selected and fails with XX001 and no fallback (card
//!   A-7d), which an invalid copy, one the open passes over, would not do.
//! - **The places** (§2.2, §2.4): meta `k`'s primary copy is in slot
//!   `1 + k mod 2` and its replica in parity `k mod 2`'s block of the
//!   replica region; the slots that overlap the region are reserved.
//! - **The boot-id serial order** (§12.5), which the tie rule uses (§2.2).
//!
//! Nothing here does I/O (§2: all I/O goes through `nucleus-env`).
//!
//! # Page 0's record (§2.1)
//!
//! The first 41 bytes of the file's first 4096 bytes:
//!
//! | offset | size | field |
//! |---|---|---|
//! | 0 | 8 | magic `NUCLEUS\0` |
//! | 8 | 4 | format_version |
//! | 12 | 4 | page_size |
//! | 16 | 1 | checksum_alg |
//! | 17 | 4 | wal_seg_size |
//! | 21 | 16 | creation_uuid |
//! | 37 | 4 | crc: CRC32C of bytes 0..37 |
//!
//! # The meta record (§2.2)
//!
//! The first 167 bytes of a meta slot or of a replica block:
//!
//! | offset | size | field |
//! |---|---|---|
//! | 0 | 8 | magic `NUCMETA\0` |
//! | 8 | 4 | format_version |
//! | 12 | 1 | copy (0 primary, 1 replica) |
//! | 13 | 4 | page_size |
//! | 17 | 1 | checksum_alg |
//! | 18 | 4 | wal_seg_size |
//! | 22 | 16 | creation_uuid |
//! | 38 | 8 | ckpt_id |
//! | 46 | 8 | T |
//! | 54 | 8 | gc_w |
//! | 62 | 4 | epoch |
//! | 66 | 16 | map_root (slot u64 @66, epoch u32 @74, crc u32 @78) |
//! | 82 | 1 | map_level |
//! | 83 | 16 | free_root (slot @83, epoch @91, crc @95) |
//! | 99 | 8 | dir_root |
//! | 107 | 8 | next_lpid |
//! | 115 | 8 | slot_count |
//! | 123 | 8 | wal_start_lsn |
//! | 131 | 8 | wal_start_prev |
//! | 139 | 4 | wal_start_prev_crc |
//! | 143 | 8 | wal_ckpt_end |
//! | 151 | 8 | ts_ckpt_hi |
//! | 159 | 4 | boot_id |
//! | 163 | 4 | crc: CRC32C of bytes 0..163 |

use crate::codec::{Reader, Writer, get_u32};
use crate::crc::crc32c;
use crate::store::StoreError;

/// The format version this build writes and opens. §2.1 gives the field,
/// not its value; card A-2-0 pins 1 (a change is a format change, free
/// until A-7e's compatibility verdict).
pub const FORMAT_VERSION: u32 = 1;

/// `checksum_alg` 1: CRC32C (§2.1), the only algorithm.
pub const CHECKSUM_CRC32C: u8 = 1;

/// The allowed page sizes (§2.1).
pub const PAGE_SIZES: [u32; 5] = [4096, 8192, 16384, 32768, 65536];

/// The default page size (§2.1).
pub const DEFAULT_PAGE_SIZE: u32 = 8192;

/// The default WAL segment size, 64 MiB (§2.1).
pub const DEFAULT_WAL_SEG_SIZE: u32 = 67_108_864;

/// Page 0's magic (§2.1).
pub const PAGE0_MAGIC: [u8; 8] = *b"NUCLEUS\0";

/// The meta record's magic (§2.2).
pub const META_MAGIC: [u8; 8] = *b"NUCMETA\0";

/// Page 0 is read as the file's first 4096 bytes, whatever the page size
/// (§2.1).
pub const PAGE0_LEN: usize = 4096;

/// The length of page 0's record, its CRC included (§2.1; the module
/// doc's table).
pub const PAGE0_RECORD_LEN: usize = 41;

/// The length of the meta record, its CRC included (§2.2; the module
/// doc's table).
pub const META_LEN: usize = 167;

/// The length of a replica block (§2.4).
pub const REPLICA_BLOCK_LEN: usize = 4096;

/// The byte offsets of the replica blocks (§2.4): parity 0's at 1 MiB,
/// parity 1's at 1 MiB + 4 KiB.
pub const REPLICA_OFFSETS: [u64; 2] = [1_048_576, 1_052_672];

/// The first byte of the replica region: parity 0's block (§2.4).
const REPLICA_REGION_START: u64 = 1_048_576;

/// One past the last byte of the replica region: 1 MiB + 8 KiB, the end of
/// parity 1's block (§2.4).
const REPLICA_REGION_END: u64 = 1_056_768;

/// Where page 0's CRC sits: after every byte it covers (§2.1).
const PAGE0_CRC_OFFSET: usize = PAGE0_RECORD_LEN - 4;

/// Where the meta record's CRC sits: after every byte it covers (§2.2).
const META_CRC_OFFSET: usize = META_LEN - 4;

/// Whether `ps` is an allowed page size (§2.1).
pub fn is_page_size(ps: u32) -> bool {
    PAGE_SIZES.contains(&ps)
}

/// The file's static fields: page 0's, and every meta copy's (§2.1; §2.2
/// "The file's identity").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Identity {
    /// The format version: [`FORMAT_VERSION`] in a well-formed record.
    pub format_version: u32,
    /// The page size: one of [`PAGE_SIZES`].
    pub page_size: u32,
    /// The checksum algorithm: [`CHECKSUM_CRC32C`].
    pub checksum_alg: u8,
    /// Bytes per WAL segment; not 0.
    pub wal_seg_size: u32,
    /// The file's creation UUID, written once at creation (§2.3).
    pub creation_uuid: [u8; 16],
}

/// A page reference: `(slot u64, epoch u32, crc u32)`, 16 bytes, slot first
/// (§2.2 `map_root` and `free_root`; §4.1's map entries have the same shape
/// and reuse this type, card A-2; §3.3 checks a page read against it).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Ref {
    /// The slot holding the page; 0 for no page, since slot 0 is page 0 and
    /// never a page's home (§1).
    pub slot: u64,
    /// The page's content epoch, low 32 bits (§1, §3.1).
    pub epoch: u32,
    /// The page's header `crc` (§3.1).
    pub crc: u32,
}

impl Ref {
    /// The null reference: all zero.
    pub const NULL: Ref = Ref {
        slot: 0,
        epoch: 0,
        crc: 0,
    };

    /// Whether this references no page: its slot is 0.
    pub fn is_null(&self) -> bool {
        self.slot == 0
    }

    /// The 16 bytes: `slot`, `epoch`, `crc`, little-endian.
    pub fn encode(&self) -> [u8; 16] {
        let [s0, s1, s2, s3, s4, s5, s6, s7] = self.slot.to_le_bytes();
        let [e0, e1, e2, e3] = self.epoch.to_le_bytes();
        let [c0, c1, c2, c3] = self.crc.to_le_bytes();
        [
            s0, s1, s2, s3, s4, s5, s6, s7, e0, e1, e2, e3, c0, c1, c2, c3,
        ]
    }

    /// The reference 16 bytes hold; every 16 bytes are some reference, so
    /// this cannot fail (§3.3's checks judge it when a page is read).
    pub fn decode(b: &[u8; 16]) -> Ref {
        let [
            s0,
            s1,
            s2,
            s3,
            s4,
            s5,
            s6,
            s7,
            e0,
            e1,
            e2,
            e3,
            c0,
            c1,
            c2,
            c3,
        ] = *b;
        Ref {
            slot: u64::from_le_bytes([s0, s1, s2, s3, s4, s5, s6, s7]),
            epoch: u32::from_le_bytes([e0, e1, e2, e3]),
            crc: u32::from_le_bytes([c0, c1, c2, c3]),
        }
    }
}

/// The meta record (§2.2): every field but `magic`, `copy` and `crc`, with
/// the spec's names (`T` as `t`). Two encoded copies of one meta therefore
/// differ "in a byte other than `copy` and `crc`" exactly when their `Meta`
/// values differ, which is the tie rule's test (§2.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Meta {
    /// The file's static fields, page 0's (§2.1).
    pub identity: Identity,
    /// `k`.
    pub ckpt_id: u64,
    /// `T`: the apply frontier at the flip of checkpoint `k` (§13 step 0).
    pub t: u64,
    /// The largest `W` handed to any GC mutation in image `k` (§10.5).
    pub gc_w: u64,
    /// `k`'s low 32 bits; compared for equality only.
    pub epoch: u32,
    /// The page-map root (§4.1).
    pub map_root: Ref,
    /// The page-map root's radix level, counted from the leaf level (0); at
    /// least 1 (§4.1).
    pub map_level: u8,
    /// The free-set root (§4.3).
    pub free_root: Ref,
    /// The LPID of the tree directory's root (§6.8).
    pub dir_root: u64,
    /// One past the highest LPID ever allocated.
    pub next_lpid: u64,
    /// The file length in slots covered by image `k`'s free set.
    pub slot_count: u64,
    /// The replay start (§12.6).
    pub wal_start_lsn: u64,
    /// The start LSN of the record before `wal_start_lsn` (§12.2
    /// `prev_lsn`); all-ones if none, as in meta 0.
    pub wal_start_prev: u64,
    /// That record's `crc` (§12.2 `prev_crc`); 0 if none, as in meta 0.
    pub wal_start_prev_crc: u32,
    /// The WAL append position read at the flip of checkpoint `k` (§13 step
    /// 0).
    pub wal_ckpt_end: u64,
    /// The `commit_ts` of the last `Commit` or `CommitMarker` appended
    /// before that flip, or the frontier the boot started from; at least
    /// `T` in a meta the open accepts (§14 step 1).
    pub ts_ckpt_hi: u64,
    /// The boot id of the session that wrote meta `k` (C-T0 v8 §2.5); 0 in
    /// meta 0 (§2.3).
    pub boot_id: u32,
}

/// The `copy` byte (§2.2): which place a copy was written for. It only
/// makes the two copies' bytes differ (§2.4); the open rule ignores it.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetaCopy {
    /// 0: written for slot `1 + k mod 2`.
    Primary = 0,
    /// 1: written for its parity's replica block.
    Replica = 1,
}

/// An XX001 error.
fn corrupt(msg: String) -> StoreError {
    StoreError::Corrupt(msg)
}

/// The first `len` bytes of `block`, the whole record; no byte after it is
/// examined.
fn whole_record<'a>(what: &str, block: &'a [u8], len: usize) -> Result<&'a [u8], StoreError> {
    block.get(..len).ok_or_else(|| {
        corrupt(format!(
            "{what}: a block of {} bytes is shorter than the {len}-byte record",
            block.len()
        ))
    })
}

/// Checks a record's magic.
fn check_magic(what: &str, found: [u8; 8], want: [u8; 8]) -> Result<(), StoreError> {
    if found == want {
        Ok(())
    } else {
        Err(corrupt(format!("{what}: bad magic {found:02x?}")))
    }
}

/// Checks a record's CRC: the `u32` at `crc_at` is the CRC32C of every byte
/// before it (§2.1, §2.2: "CRC32C of the bytes above").
fn check_crc(what: &str, record: &[u8], crc_at: usize) -> Result<(), StoreError> {
    let stored = get_u32(record, crc_at)?;
    let computed = crc32c(0, Reader::new(record).bytes(crc_at)?);
    if stored == computed {
        Ok(())
    } else {
        Err(corrupt(format!(
            "{what}: crc {stored:#010x} does not match the bytes' crc {computed:#010x}"
        )))
    }
}

/// Checks the static fields' domains (§2.1; card A-2-0 Work 3).
fn check_identity(what: &str, id: &Identity) -> Result<(), StoreError> {
    if id.format_version != FORMAT_VERSION {
        return Err(corrupt(format!(
            "{what}: format_version {} is not {FORMAT_VERSION}",
            id.format_version
        )));
    }
    if !is_page_size(id.page_size) {
        return Err(corrupt(format!(
            "{what}: page_size {} is not one of {PAGE_SIZES:?}",
            id.page_size
        )));
    }
    if id.checksum_alg != CHECKSUM_CRC32C {
        return Err(corrupt(format!(
            "{what}: checksum_alg {} is not {CHECKSUM_CRC32C} (CRC32C)",
            id.checksum_alg
        )));
    }
    if id.wal_seg_size == 0 {
        return Err(corrupt(format!("{what}: wal_seg_size is 0")));
    }
    Ok(())
}

/// Writes the static fields that follow `format_version` (and, in a meta,
/// `copy`): `page_size`, `checksum_alg`, `wal_seg_size`, `creation_uuid`.
fn write_static(w: &mut Writer, id: &Identity) {
    w.u32(id.page_size);
    w.u8(id.checksum_alg);
    w.u32(id.wal_seg_size);
    w.bytes(&id.creation_uuid);
}

/// Reads what [`write_static`] writes, with the `format_version` read
/// before it.
fn read_static(r: &mut Reader<'_>, format_version: u32) -> Result<Identity, StoreError> {
    Ok(Identity {
        format_version,
        page_size: r.u32()?,
        checksum_alg: r.u8()?,
        wal_seg_size: r.u32()?,
        creation_uuid: r.array()?,
    })
}

/// Seals a record: appends the CRC32C of every byte written (§2.1, §2.2:
/// "CRC32C of the bytes above") and places the record at the start of a
/// zeroed block of `N` bytes ("The rest ... is zero").
fn seal<const N: usize>(w: Writer) -> [u8; N] {
    let mut record = w.into_vec();
    let crc = crc32c(0, &record);
    record.extend_from_slice(&crc.to_le_bytes());
    let mut block = [0u8; N];
    for (d, s) in block.iter_mut().zip(record) {
        *d = s;
    }
    block
}

/// Page 0's 4096 bytes for `id`: the record (the module doc's table) and
/// zeros (§2.1). Infallible: it writes whatever values it is given, so
/// tests can build malformed but CRC-valid blocks; the creator validates
/// what it writes (§2.3).
pub fn encode_page0(id: &Identity) -> [u8; PAGE0_LEN] {
    let mut w = Writer::with_capacity(PAGE0_RECORD_LEN);
    w.bytes(&PAGE0_MAGIC);
    w.u32(id.format_version);
    write_static(&mut w, id);
    seal(w)
}

/// The file's identity from page 0's record at the start of `block` (the
/// file's first 4096 bytes, or any block that starts with the record).
/// Every failure is `StoreError::Corrupt`, in the module doc's order: the
/// whole record, the magic, the CRC, the field domains.
pub fn decode_page0(block: &[u8]) -> Result<Identity, StoreError> {
    let record = whole_record("page 0", block, PAGE0_RECORD_LEN)?;
    let mut r = Reader::new(record);
    check_magic("page 0", r.array()?, PAGE0_MAGIC)?;
    check_crc("page 0", record, PAGE0_CRC_OFFSET)?;
    let format_version = r.u32()?;
    let identity = read_static(&mut r, format_version)?;
    r.u32()?; // the crc, checked above
    r.finish()?;
    check_identity("page 0", &identity)?;
    Ok(identity)
}

/// The meta record's 167 bytes for `meta` and `copy` (the module doc's
/// table): the first 167 bytes of a meta slot or a replica block, whose
/// rest is zero (§2.2). Infallible: it writes whatever values it is given,
/// so tests can build malformed but CRC-valid copies; the writer (§13 step
/// 4, card A-7c) validates what it writes.
pub fn encode_meta(meta: &Meta, copy: MetaCopy) -> [u8; META_LEN] {
    let mut w = Writer::with_capacity(META_LEN);
    w.bytes(&META_MAGIC);
    w.u32(meta.identity.format_version);
    w.u8(copy as u8);
    write_static(&mut w, &meta.identity);
    w.u64(meta.ckpt_id);
    w.u64(meta.t);
    w.u64(meta.gc_w);
    w.u32(meta.epoch);
    w.bytes(&meta.map_root.encode());
    w.u8(meta.map_level);
    w.bytes(&meta.free_root.encode());
    w.u64(meta.dir_root);
    w.u64(meta.next_lpid);
    w.u64(meta.slot_count);
    w.u64(meta.wal_start_lsn);
    w.u64(meta.wal_start_prev);
    w.u32(meta.wal_start_prev_crc);
    w.u64(meta.wal_ckpt_end);
    w.u64(meta.ts_ckpt_hi);
    w.u32(meta.boot_id);
    seal(w)
}

/// The meta and its `copy` byte from the record at the start of `block` (a
/// slot, a replica block, or any block that starts with the record). Every
/// failure is `StoreError::Corrupt`, in the module doc's order: the whole
/// record, the magic, the CRC, the field domains. §14 step 1's range check
/// is not made here (module doc).
pub fn decode_meta(block: &[u8]) -> Result<(Meta, MetaCopy), StoreError> {
    let record = whole_record("meta", block, META_LEN)?;
    let mut r = Reader::new(record);
    check_magic("meta", r.array()?, META_MAGIC)?;
    check_crc("meta", record, META_CRC_OFFSET)?;
    let format_version = r.u32()?;
    let copy = r.u8()?;
    let identity = read_static(&mut r, format_version)?;
    let ckpt_id = r.u64()?;
    let t = r.u64()?;
    let gc_w = r.u64()?;
    let epoch = r.u32()?;
    let map_root = Ref::decode(&r.array()?);
    let map_level = r.u8()?;
    let free_root = Ref::decode(&r.array()?);
    let dir_root = r.u64()?;
    let next_lpid = r.u64()?;
    let slot_count = r.u64()?;
    let wal_start_lsn = r.u64()?;
    let wal_start_prev = r.u64()?;
    let wal_start_prev_crc = r.u32()?;
    let wal_ckpt_end = r.u64()?;
    let ts_ckpt_hi = r.u64()?;
    let boot_id = r.u32()?;
    r.u32()?; // the crc, checked above
    r.finish()?;
    check_identity("meta", &identity)?;
    let copy = match copy {
        0 => MetaCopy::Primary,
        1 => MetaCopy::Replica,
        other => {
            return Err(corrupt(format!(
                "meta: copy byte {other} is neither 0 (primary) nor 1 (replica)"
            )));
        }
    };
    if map_level < 1 {
        return Err(corrupt(format!(
            "meta: map_level {map_level} is below 1 (§4.1)"
        )));
    }
    // §2.2: "epoch u32 k (low 32 bits)".
    if u64::from(epoch) != ckpt_id & u64::from(u32::MAX) {
        return Err(corrupt(format!(
            "meta: epoch {epoch} is not the low 32 bits of ckpt_id {ckpt_id}"
        )));
    }
    let meta = Meta {
        identity,
        ckpt_id,
        t,
        gc_w,
        epoch,
        map_root,
        map_level,
        free_root,
        dir_root,
        next_lpid,
        slot_count,
        wal_start_lsn,
        wal_start_prev,
        wal_start_prev_crc,
        wal_ckpt_end,
        ts_ckpt_hi,
        boot_id,
    };
    Ok((meta, copy))
}

/// The parity of meta `ckpt_id`, `ckpt_id mod 2`: the index of its places,
/// slot `1 + parity` and `REPLICA_OFFSETS[parity]` (§2.2, §2.4).
pub fn meta_parity(ckpt_id: u64) -> usize {
    usize::from(ckpt_id % 2 == 1)
}

/// The primary place of meta `ckpt_id`: slot `1 + ckpt_id mod 2` (§2.2).
pub fn meta_slot(ckpt_id: u64) -> u64 {
    1 + ckpt_id % 2
}

/// The reserved slots at `page_size` (§2.4): those that overlap the replica
/// region, from `⌊1 MiB / page_size⌋` up to but not including
/// `⌈(1 MiB + 8 KiB) / page_size⌉`. Never a page's home, and never free.
/// `Invalid` for a size outside [`PAGE_SIZES`] (a caller's misuse).
pub fn reserved_slots(page_size: u32) -> Result<core::ops::Range<u64>, StoreError> {
    if !is_page_size(page_size) {
        return Err(StoreError::Invalid(format!(
            "page size {page_size} is not one of {PAGE_SIZES:?}"
        )));
    }
    let ps = u64::from(page_size);
    let first = REPLICA_REGION_START / ps;
    let end = REPLICA_REGION_END.div_ceil(ps);
    Ok(first..end)
}

/// The fewest slots a file is created with at `page_size`: the end of the
/// reserved slots, so that every reserved slot lies in `[3, slot_count)`
/// (§2.3 step 1, §2.4). `Invalid` for a size outside [`PAGE_SIZES`].
pub fn min_file_slots(page_size: u32) -> Result<u64, StoreError> {
    Ok(reserved_slots(page_size)?.end)
}

/// Whether boot id `a` is older than `b` in serial-number order (§12.5):
/// "`a` is older than `b` iff `(a − b) mod 2^32 ≥ 2^31`". Equal ids are
/// neither; two ids exactly 2^31 apart are each older than the other, so
/// the tie rule finds neither newer (card A-2-0, open point 4).
pub fn boot_id_older(a: u32, b: u32) -> bool {
    a.wrapping_sub(b) >= 1 << 31
}
