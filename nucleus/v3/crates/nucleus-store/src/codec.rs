//! The bounded byte codec (card A-2-0, Work 2): little-endian reads and
//! writes over byte buffers, for this crate's decoders and encoders. Page 0
//! and the meta record (`meta`, card A-2-0) use it now; the page header and
//! map pages (card A-2) and the WAL record (card A-6a) use it without
//! editing it.
//!
//! Invariants this module owns:
//! - S-STORE §3.1 "A decoder never panics: every malformed byte sequence is
//!   XX001"; every read past the end of its buffer is `StoreError::Corrupt`.
//! - A read that fails leaves its [`Reader`] where it was, so the caller can
//!   use no partial state.
//! - Every index computation is checked (`checked_add`): an offset near
//!   `usize::MAX` is `Corrupt` on a read and `Invalid` on a write, never an
//!   overflow.
//! - Writing past a buffer is the caller's bug, not damaged data, so the
//!   `put_*` functions return `StoreError::Invalid`, never `Corrupt`.
//!
//! Byte order: little-endian throughout. S-STORE fixes every on-disk
//! field's size and order; card A-2-0 pins the byte order of every on-disk
//! integer except where the spec says big-endian, which is only key
//! encodings (§6.8, §7.2), and those do not go through this module.
//!
//! Some helpers are unused until A-2 and A-6a land (their records have the
//! `u16` fields and in-place writes that page 0 and the meta record lack).
//! They carry `#[allow(dead_code)]`, not `#[expect(dead_code)]`: an expect
//! would turn into a warning the moment A-2 or A-6a used the helper, and
//! those cards may not edit this file.

use crate::store::StoreError;

/// The error of a read of `n` bytes at `off` past the end of a `len`-byte
/// buffer (S-STORE §3.1: damaged or short data is XX001).
fn past_end(n: usize, off: usize, len: usize) -> StoreError {
    StoreError::Corrupt(format!(
        "read of {n} bytes at offset {off} runs past the end of a {len}-byte buffer"
    ))
}

/// A bounded little-endian reader over a byte slice, with a position.
#[derive(Debug, Clone)]
pub(crate) struct Reader<'a> {
    /// The bytes being read.
    buf: &'a [u8],
    /// The number of bytes read so far; never above `buf.len()`.
    pos: usize,
}

impl<'a> Reader<'a> {
    /// A reader at the start of `buf`.
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    /// The number of bytes read so far.
    #[allow(dead_code)] // first used by A-2 or A-6a (module doc)
    pub(crate) fn pos(&self) -> usize {
        self.pos
    }

    /// The number of bytes left to read.
    pub(crate) fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }

    /// The `n` bytes at the position and the position after them, without
    /// moving, so that a failed read leaves the reader where it was.
    fn peek(&self, n: usize) -> Result<(&'a [u8], usize), StoreError> {
        self.pos
            .checked_add(n)
            .and_then(|end| Some((self.buf.get(self.pos..end)?, end)))
            .ok_or_else(|| past_end(n, self.pos, self.buf.len()))
    }

    /// The next `n` bytes, borrowed from the buffer.
    pub(crate) fn bytes(&mut self, n: usize) -> Result<&'a [u8], StoreError> {
        let (out, end) = self.peek(n)?;
        self.pos = end;
        Ok(out)
    }

    /// The next `N` bytes, copied into an array.
    pub(crate) fn array<const N: usize>(&mut self) -> Result<[u8; N], StoreError> {
        let (b, end) = self.peek(N)?;
        // `peek` returned exactly N bytes, so this conversion cannot fail.
        let out = b
            .first_chunk::<N>()
            .copied()
            .ok_or_else(|| past_end(N, self.pos, self.buf.len()))?;
        self.pos = end;
        Ok(out)
    }

    /// The next byte.
    pub(crate) fn u8(&mut self) -> Result<u8, StoreError> {
        self.array().map(u8::from_le_bytes)
    }

    /// The next little-endian `u16`.
    #[allow(dead_code)] // first used by A-2 or A-6a (module doc)
    pub(crate) fn u16(&mut self) -> Result<u16, StoreError> {
        self.array().map(u16::from_le_bytes)
    }

    /// The next little-endian `u32`.
    pub(crate) fn u32(&mut self) -> Result<u32, StoreError> {
        self.array().map(u32::from_le_bytes)
    }

    /// The next little-endian `u64`.
    pub(crate) fn u64(&mut self) -> Result<u64, StoreError> {
        self.array().map(u64::from_le_bytes)
    }

    /// Ends the read: `Corrupt` if any byte is left, so a record's fields
    /// must account for every byte of it.
    pub(crate) fn finish(self) -> Result<(), StoreError> {
        match self.remaining() {
            0 => Ok(()),
            left => Err(StoreError::Corrupt(format!(
                "{left} bytes left after the last field, at offset {} of a {}-byte record",
                self.pos,
                self.buf.len()
            ))),
        }
    }
}

/// A little-endian writer that appends to a growing `Vec<u8>`; infallible.
#[derive(Debug, Clone, Default)]
pub(crate) struct Writer {
    /// The bytes written so far.
    buf: Vec<u8>,
}

impl Writer {
    /// An empty writer.
    #[allow(dead_code)] // first used by A-2 or A-6a (module doc)
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// An empty writer with room for `n` bytes.
    pub(crate) fn with_capacity(n: usize) -> Self {
        Self {
            buf: Vec::with_capacity(n),
        }
    }

    /// Appends one byte.
    pub(crate) fn u8(&mut self, v: u8) {
        self.buf.push(v);
    }

    /// Appends a little-endian `u16`.
    #[allow(dead_code)] // first used by A-2 or A-6a (module doc)
    pub(crate) fn u16(&mut self, v: u16) {
        self.bytes(&v.to_le_bytes());
    }

    /// Appends a little-endian `u32`.
    pub(crate) fn u32(&mut self, v: u32) {
        self.bytes(&v.to_le_bytes());
    }

    /// Appends a little-endian `u64`.
    pub(crate) fn u64(&mut self, v: u64) {
        self.bytes(&v.to_le_bytes());
    }

    /// Appends `b`.
    pub(crate) fn bytes(&mut self, b: &[u8]) {
        self.buf.extend_from_slice(b);
    }

    /// The number of bytes written so far.
    #[allow(dead_code)] // first used by A-2 or A-6a (module doc)
    pub(crate) fn len(&self) -> usize {
        self.buf.len()
    }

    /// The bytes written.
    pub(crate) fn into_vec(self) -> Vec<u8> {
        self.buf
    }
}

/// The `N` bytes of `buf` at `off`; `Corrupt` past its end (checked).
fn get<const N: usize>(buf: &[u8], off: usize) -> Result<[u8; N], StoreError> {
    off.checked_add(N)
        .and_then(|end| buf.get(off..end))
        .and_then(|b| b.first_chunk::<N>().copied())
        .ok_or_else(|| past_end(N, off, buf.len()))
}

/// Writes `v` into `buf` at `off`; `Invalid` past its end (checked).
fn put<const N: usize>(buf: &mut [u8], off: usize, v: [u8; N]) -> Result<(), StoreError> {
    let len = buf.len();
    let dst = off
        .checked_add(N)
        .and_then(|end| buf.get_mut(off..end))
        .ok_or_else(|| {
            StoreError::Invalid(format!(
                "write of {N} bytes at offset {off} runs past the end of a {len}-byte buffer"
            ))
        })?;
    for (d, s) in dst.iter_mut().zip(v) {
        *d = s;
    }
    Ok(())
}

/// The byte at `off`; `Corrupt` out of bounds.
#[allow(dead_code)] // first used by A-2 or A-6a (module doc)
pub(crate) fn get_u8(buf: &[u8], off: usize) -> Result<u8, StoreError> {
    get(buf, off).map(u8::from_le_bytes)
}

/// The little-endian `u16` at `off`; `Corrupt` out of bounds.
#[allow(dead_code)] // first used by A-2 or A-6a (module doc)
pub(crate) fn get_u16(buf: &[u8], off: usize) -> Result<u16, StoreError> {
    get(buf, off).map(u16::from_le_bytes)
}

/// The little-endian `u32` at `off`; `Corrupt` out of bounds.
pub(crate) fn get_u32(buf: &[u8], off: usize) -> Result<u32, StoreError> {
    get(buf, off).map(u32::from_le_bytes)
}

/// The little-endian `u64` at `off`; `Corrupt` out of bounds.
#[allow(dead_code)] // first used by A-2 or A-6a (module doc)
pub(crate) fn get_u64(buf: &[u8], off: usize) -> Result<u64, StoreError> {
    get(buf, off).map(u64::from_le_bytes)
}

/// Writes the byte `v` at `off`; `Invalid` out of bounds.
#[allow(dead_code)] // first used by A-2 or A-6a (module doc)
pub(crate) fn put_u8(buf: &mut [u8], off: usize, v: u8) -> Result<(), StoreError> {
    put(buf, off, v.to_le_bytes())
}

/// Writes `v` little-endian at `off`; `Invalid` out of bounds.
#[allow(dead_code)] // first used by A-2 or A-6a (module doc)
pub(crate) fn put_u16(buf: &mut [u8], off: usize, v: u16) -> Result<(), StoreError> {
    put(buf, off, v.to_le_bytes())
}

/// Writes `v` little-endian at `off`; `Invalid` out of bounds.
#[allow(dead_code)] // first used by A-2 or A-6a (module doc)
pub(crate) fn put_u32(buf: &mut [u8], off: usize, v: u32) -> Result<(), StoreError> {
    put(buf, off, v.to_le_bytes())
}

/// Writes `v` little-endian at `off`; `Invalid` out of bounds.
#[allow(dead_code)] // first used by A-2 or A-6a (module doc)
pub(crate) fn put_u64(buf: &mut [u8], off: usize, v: u64) -> Result<(), StoreError> {
    put(buf, off, v.to_le_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `len` bytes counting up from 1, so every width reads a value whose
    /// byte order shows.
    fn counting(len: usize) -> Vec<u8> {
        (1..=len).map(|i| (i % 251) as u8).collect()
    }

    fn is_corrupt<T>(r: Result<T, StoreError>) -> bool {
        matches!(r, Err(StoreError::Corrupt(_)))
    }

    fn is_invalid<T>(r: Result<T, StoreError>) -> bool {
        matches!(r, Err(StoreError::Invalid(_)))
    }

    /// Each width read exactly fitting its buffer, and one byte short, by
    /// the reader and by the `get_*` functions; little-endian values.
    #[test]
    fn reads_at_every_boundary() {
        let b = counting(8);
        // Exactly fitting.
        assert_eq!(Reader::new(b.get(..1).unwrap()).u8().unwrap(), 0x01);
        assert_eq!(Reader::new(b.get(..2).unwrap()).u16().unwrap(), 0x0201);
        assert_eq!(Reader::new(b.get(..4).unwrap()).u32().unwrap(), 0x0403_0201);
        assert_eq!(
            Reader::new(b.get(..8).unwrap()).u64().unwrap(),
            0x0807_0605_0403_0201
        );
        assert_eq!(get_u8(&b, 7).unwrap(), 0x08);
        assert_eq!(get_u16(&b, 6).unwrap(), 0x0807);
        assert_eq!(get_u32(&b, 4).unwrap(), 0x0807_0605);
        assert_eq!(get_u64(&b, 0).unwrap(), 0x0807_0605_0403_0201);
        // One byte short.
        assert!(is_corrupt(Reader::new(&[]).u8()));
        assert!(is_corrupt(Reader::new(b.get(..1).unwrap()).u16()));
        assert!(is_corrupt(Reader::new(b.get(..3).unwrap()).u32()));
        assert!(is_corrupt(Reader::new(b.get(..7).unwrap()).u64()));
        assert!(is_corrupt(get_u8(&b, 8)));
        assert!(is_corrupt(get_u16(&b, 7)));
        assert!(is_corrupt(get_u32(&b, 5)));
        assert!(is_corrupt(get_u64(&b, 1)));
        // `bytes` and `array` at the boundary.
        assert_eq!(Reader::new(&b).bytes(8).unwrap(), b.as_slice());
        assert!(is_corrupt(Reader::new(&b).bytes(9)));
        assert_eq!(Reader::new(&b).array::<3>().unwrap(), [1, 2, 3]);
        assert!(is_corrupt(Reader::new(&b).array::<9>()));
        assert_eq!(Reader::new(&b).bytes(0).unwrap(), &[] as &[u8]);
    }

    /// A sequence of reads walks the buffer in order; a failed read leaves
    /// the position where it was, and the next read that fits succeeds.
    #[test]
    fn failed_read_leaves_no_partial_state() {
        let b = counting(7);
        let mut r = Reader::new(&b);
        assert_eq!(r.u8().unwrap(), 0x01);
        assert_eq!(r.u16().unwrap(), 0x0302);
        assert_eq!((r.pos(), r.remaining()), (3, 4));
        assert!(is_corrupt(r.u64()));
        assert!(is_corrupt(r.bytes(5)));
        assert!(is_corrupt(r.array::<5>()));
        assert_eq!((r.pos(), r.remaining()), (3, 4));
        assert_eq!(r.u32().unwrap(), 0x0706_0504);
        assert_eq!((r.pos(), r.remaining()), (7, 0));
        assert!(is_corrupt(r.u8()));
        assert!(r.finish().is_ok());
    }

    /// `finish` refuses leftovers and accepts an exactly consumed buffer.
    #[test]
    fn finish_with_and_without_leftovers() {
        let b = counting(5);
        let mut r = Reader::new(&b);
        r.u32().unwrap();
        assert!(is_corrupt(r.clone().finish()));
        r.u8().unwrap();
        assert!(r.finish().is_ok());
        assert!(Reader::new(&[]).finish().is_ok());
        assert!(is_corrupt(Reader::new(&[0]).finish()));
    }

    /// Offsets at and near `usize::MAX` never overflow: reads are
    /// `Corrupt`, writes `Invalid`.
    #[test]
    fn offset_of_usize_max() {
        let mut b = counting(16);
        for off in [usize::MAX, usize::MAX - 1, usize::MAX - 7] {
            assert!(is_corrupt(get_u8(&b, off)));
            assert!(is_corrupt(get_u16(&b, off)));
            assert!(is_corrupt(get_u32(&b, off)));
            assert!(is_corrupt(get_u64(&b, off)));
            assert!(is_invalid(put_u8(&mut b, off, 0)));
            assert!(is_invalid(put_u16(&mut b, off, 0)));
            assert!(is_invalid(put_u32(&mut b, off, 0)));
            assert!(is_invalid(put_u64(&mut b, off, 0)));
        }
        let mut r = Reader::new(&b);
        r.u8().unwrap();
        assert!(is_corrupt(r.bytes(usize::MAX)));
        assert!(is_corrupt(r.bytes(usize::MAX - 1)));
        assert_eq!(r.pos(), 1);
        assert_eq!(b, counting(16));
    }

    /// Puts write little-endian inside the buffer and refuse, unchanged,
    /// anything that runs past its end.
    #[test]
    fn puts_in_and_out_of_bounds() {
        let mut b = vec![0u8; 8];
        put_u64(&mut b, 0, 0x0807_0605_0403_0201).unwrap();
        assert_eq!(b, counting(8));
        put_u32(&mut b, 4, 0xDDCC_BBAA).unwrap();
        put_u16(&mut b, 2, 0xFFEE).unwrap();
        put_u8(&mut b, 0, 0x7F).unwrap();
        assert_eq!(b, [0x7F, 0x02, 0xEE, 0xFF, 0xAA, 0xBB, 0xCC, 0xDD]);
        let before = b.clone();
        assert!(is_invalid(put_u8(&mut b, 8, 0)));
        assert!(is_invalid(put_u16(&mut b, 7, 0)));
        assert!(is_invalid(put_u32(&mut b, 5, 0)));
        assert!(is_invalid(put_u64(&mut b, 1, 0)));
        assert!(is_invalid(put_u64(&mut [], 0, 0)));
        assert_eq!(b, before);
    }

    /// The writer appends little-endian and reads back through the reader.
    #[test]
    fn writer_round_trips_through_reader() {
        let mut w = Writer::new();
        w.u8(0xAB);
        w.u16(0x0201);
        w.u32(0x0605_0403);
        w.u64(0x0E0D_0C0B_0A09_0807);
        w.bytes(b"xyz");
        assert_eq!(w.len(), 18);
        let v = w.into_vec();
        let mut r = Reader::new(&v);
        assert_eq!(r.u8().unwrap(), 0xAB);
        assert_eq!(r.u16().unwrap(), 0x0201);
        assert_eq!(r.u32().unwrap(), 0x0605_0403);
        assert_eq!(r.u64().unwrap(), 0x0E0D_0C0B_0A09_0807);
        assert_eq!(r.array::<3>().unwrap(), *b"xyz");
        assert!(r.finish().is_ok());
        let mut w = Writer::with_capacity(2);
        w.u16(0xBEEF);
        assert_eq!(w.into_vec(), [0xEF, 0xBE]);
    }
}
