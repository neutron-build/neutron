//! The two-phase seal (S-STORE §12.2's order of sealing and design note;
//! card A-6a, Work 4).
//!
//! A record's `crc` covers its `lsn`, `prev_lsn` and `prev_crc`, which are
//! known only when its position is reserved, inside the append critical
//! section (§12.4). CRC32C is affine, so the work splits in two:
//! - [`encode`](crate::wal::encode) writes the record with those fields
//!   and `crc` zero and computes the CRC of every byte after them, bytes
//!   32..`len`: the O(len) work, done before the reservation;
//! - [`Unsealed::seal`] writes the three chain fields, takes the CRC of
//!   their 20 bytes, and combines it with the first CRC by the CRC32C
//!   combine: O(log len) work, taking no lock, starting no I/O and waiting
//!   for nothing, so it may run inside the append critical section (§12.4).
//!
//! Invariants this file owns:
//! - a [`Sealed`] record's `crc` field is the CRC32C of its bytes 12..`len`
//!   (§12.2), and [`Sealed::crc`] returns it;
//! - [`crc32c_combine`] gives the CRC32C of `A ‖ B` from the CRCs of `A`
//!   and `B` and the length of `B`.
//!
//! Sealing order is the writer's discipline: a record's `crc` is final only
//! once its predecessor's is (§12.2; mutant SEAL-STALE-HEAD, card A-6c).
//! [`Unsealed::seal`] takes `prev_crc` as given; the codec cannot see the
//! order.

use crate::crc::crc32c;
use crate::store::StoreError;
use crate::wal::record::{CRC_AT, KIND_AT, LSN_AT, RECORD_HEADER_LEN};

/// The reflected Castagnoli polynomial, as in `crate::crc` (§2.1
/// `checksum_alg` 1), whose constant is private to that module.
const POLY: u32 = 0x82F6_3B78;

/// The bytes the chain fields take: `lsn` u64, `prev_lsn` u64 and
/// `prev_crc` u32, from byte 12 to byte 32.
const CHAIN_LEN: usize = KIND_AT - LSN_AT;

const _: () = assert!(CHAIN_LEN == 8 + 8 + 4);

/// The chain head at reservation (§12.2): the record's reserved position,
/// and the start and `crc` of the record reserved just before it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Chain {
    /// The record's own start position (§12.2 `lsn`).
    pub lsn: u64,
    /// The start of the record reserved just before it; all-ones if none
    /// (§12.2 `prev_lsn`; [`PREV_NONE`](crate::wal::PREV_NONE)).
    pub prev_lsn: u64,
    /// The `crc` field of that record, once sealed; 0 if none (§12.2
    /// `prev_crc`).
    pub prev_crc: u32,
}

/// A record [`encode`](crate::wal::encode) wrote with its `crc` and chain
/// fields zero, carrying the CRC32C of its bytes after the chain fields
/// (§12.2's design note). It holds at least the 53-byte header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unsealed {
    /// The whole record, `crc`, `lsn`, `prev_lsn` and `prev_crc` zero.
    bytes: Vec<u8>,
    /// `bytes.len()`, the record's `len`, at least 53.
    len: u32,
    /// The CRC32C of bytes 32..`len`.
    tail_crc: u32,
}

impl Unsealed {
    /// Wraps an encoded record and computes the CRC of its bytes after the
    /// chain fields. `Invalid` if `bytes` is shorter than a header or
    /// longer than `u32::MAX`, which `encode` never makes.
    pub(crate) fn new(bytes: Vec<u8>) -> Result<Self, StoreError> {
        let len = u32::try_from(bytes.len()).ok();
        let tail = bytes
            .get(KIND_AT..)
            .filter(|_| bytes.len() >= RECORD_HEADER_LEN);
        let (Some(len), Some(tail)) = (len, tail) else {
            return Err(StoreError::Invalid(format!(
                "an unsealed WAL record of {} bytes is not between 53 and u32::MAX bytes",
                bytes.len()
            )));
        };
        let tail_crc = crc32c(0, tail);
        Ok(Self {
            bytes,
            len,
            tail_crc,
        })
    }

    /// The record's length in bytes, its `len` field (§12.2): what the
    /// placement rule needs at reservation ([`place`](crate::wal::place)).
    #[expect(
        clippy::len_without_is_empty,
        reason = "a record is never empty: it holds at least its 53-byte header"
    )]
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Finishes the record at reservation (§12.2's design note): writes
    /// `chain`'s `lsn`, `prev_lsn` and `prev_crc` at bytes 12..32, and the
    /// CRC32C of bytes 12..`len` at bytes 8..12, computed as the combine of
    /// the CRC of those 20 bytes with the CRC [`encode`](crate::wal::encode)
    /// took of bytes 32..`len`. Its cost is a 20-byte CRC and an O(log len)
    /// combine; it takes no lock, starts no I/O and waits for nothing
    /// (§12.4).
    pub fn seal(self, chain: Chain) -> Sealed {
        let Unsealed {
            mut bytes,
            len,
            tail_crc,
        } = self;
        let mut fields = [0u8; CHAIN_LEN];
        let le = chain
            .lsn
            .to_le_bytes()
            .into_iter()
            .chain(chain.prev_lsn.to_le_bytes())
            .chain(chain.prev_crc.to_le_bytes());
        for (d, s) in fields.iter_mut().zip(le) {
            *d = s;
        }
        let tail_len = u64::from(len).saturating_sub(KIND_AT as u64);
        let crc = crc32c_combine(crc32c(0, &fields), tail_crc, tail_len);
        overwrite(&mut bytes, LSN_AT, &fields);
        overwrite(&mut bytes, CRC_AT, &crc.to_le_bytes());
        Sealed {
            bytes,
            crc,
            lsn: chain.lsn,
        }
    }
}

/// A finished record: every field written, its `crc` the CRC32C of bytes
/// 12..`len` (§12.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sealed {
    /// The whole record.
    bytes: Vec<u8>,
    /// Its `crc` field.
    crc: u32,
    /// Its `lsn` field.
    lsn: u64,
}

impl Sealed {
    /// The record's bytes, ready to be written at its `lsn` (§12.2).
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// The record's bytes, owned.
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    /// The record's `crc` field: the value the next record's `prev_crc`
    /// takes, once this one is sealed (§12.2, the chain head).
    pub fn crc(&self) -> u32 {
        self.crc
    }

    /// The record's `lsn`, its reserved position (§12.2).
    pub fn lsn(&self) -> u64 {
        self.lsn
    }
}

/// Writes `src` over `dst` from byte `at`. The caller keeps `at +
/// src.len()` within `dst`: an [`Unsealed`] record holds its whole header.
fn overwrite(dst: &mut [u8], at: usize, src: &[u8]) {
    for (d, s) in dst.iter_mut().skip(at).zip(src) {
        *d = *s;
    }
}

/// The CRC32C of `A ‖ B` from `crc1 = crc32c(0, A)`, `crc2 = crc32c(0, B)`
/// and `len2 = |B|` (card A-6a, Work 4).
///
/// zlib's `crc32_combine` algorithm, written new for the reflected
/// Castagnoli polynomial: running `|B|` zero bytes through the CRC register
/// is linear over GF(2), so it is a 32 × 32 matrix, built here by repeated
/// squaring from the operator for one zero bit and applied to `crc1` for
/// each set bit of `len2`; the pre- and post-inversion that both CRCs carry
/// cancel, leaving that product XOR `crc2`. Its cost is O(log `len2`)
/// matrix squarings of 32 rows each, and it takes no lock, starts no I/O
/// and waits for nothing.
fn crc32c_combine(crc1: u32, crc2: u32, len2: u64) -> u32 {
    // The operator for one zero bit: row 0 is the polynomial, and row n
    // (n ≥ 1) is 1 << (n − 1).
    let mut odd = [0u32; 32];
    let rows = core::iter::once(POLY).chain((0..31).map(|n| 1u32 << n));
    for (row, v) in odd.iter_mut().zip(rows) {
        *row = v;
    }
    // Two zero bits, then four.
    let mut even = gf2_square(&odd);
    odd = gf2_square(&even);
    let mut crc1 = crc1;
    let mut len2 = len2;
    // Each pass squares the operator once more (the first gives one zero
    // byte) and applies it when the matching bit of `len2` is set, with
    // `even` and `odd` taking turns.
    while len2 != 0 {
        even = gf2_square(&odd);
        if len2 & 1 != 0 {
            crc1 = gf2_times(&even, crc1);
        }
        len2 >>= 1;
        if len2 == 0 {
            break;
        }
        odd = gf2_square(&even);
        if len2 & 1 != 0 {
            crc1 = gf2_times(&odd, crc1);
        }
        len2 >>= 1;
    }
    crc1 ^ crc2
}

/// The product of the 32 × 32 GF(2) matrix `mat` (row `n` is the image of
/// bit `n`) with the vector `vec`.
fn gf2_times(mat: &[u32; 32], vec: u32) -> u32 {
    let mut sum = 0;
    let mut vec = vec;
    for row in mat {
        if vec == 0 {
            break;
        }
        if vec & 1 != 0 {
            sum ^= row;
        }
        vec >>= 1;
    }
    sum
}

/// The square of the 32 × 32 GF(2) matrix `mat`.
fn gf2_square(mat: &[u32; 32]) -> [u32; 32] {
    let mut square = [0u32; 32];
    for (s, row) in square.iter_mut().zip(mat) {
        *s = gf2_times(mat, *row);
    }
    square
}
