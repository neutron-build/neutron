//! LZ4-style fast compression for wire protocol (Phase 3).
//!
//! Provides fast compression/decompression for data in transit between
//! client/server and cluster nodes. Reduces bandwidth, latency, and
//! egress costs with minimal CPU overhead.

use std::fmt;

// ============================================================================
// Error type
// ============================================================================

/// Errors that can occur during compression or decompression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompressionError {
    /// Input data is too short or otherwise malformed.
    InvalidInput,
    /// Compressed data is internally inconsistent (bad offset, truncated token).
    CorruptedData,
    /// Decompressed output would exceed the declared original size.
    BufferOverflow,
}

impl fmt::Display for CompressionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompressionError::InvalidInput => write!(f, "invalid input"),
            CompressionError::CorruptedData => write!(f, "corrupted compressed data"),
            CompressionError::BufferOverflow => {
                write!(f, "decompressed data exceeds declared size")
            }
        }
    }
}

impl std::error::Error for CompressionError {}

// ============================================================================
// Constants
// ============================================================================

/// Minimum match length for LZ4-style encoding.
const MIN_MATCH: usize = 4;

/// Size of the sliding window (64 KB).
const WINDOW_SIZE: usize = 65535;

/// Number of entries in the hash table (power of 2 for fast modulo).
const HASH_TABLE_SIZE: usize = 1 << 14; // 16384

/// Size of the original-length header prepended to compressed output.
const HEADER_SIZE: usize = 4;

// ============================================================================
// Compression
// ============================================================================

/// Hash 4 bytes into a hash-table index.
#[inline]
fn hash4(data: &[u8], pos: usize) -> usize {
    debug_assert!(pos + 4 <= data.len());
    let v = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
    // Knuth multiplicative hash, take top bits.
    ((v.wrapping_mul(2654435761)) >> 18) as usize & (HASH_TABLE_SIZE - 1)
}

/// Count matching bytes starting at positions `a` and `b` within `data`,
/// up to `limit` bytes.
#[inline]
fn match_length(data: &[u8], a: usize, b: usize, limit: usize) -> usize {
    let mut len = 0;
    while len < limit
        && a + len < data.len()
        && b + len < data.len()
        && data[a + len] == data[b + len]
    {
        len += 1;
    }
    len
}

/// Write an extended length value (for literal_len or match_len >= 15).
/// The base 15 is already encoded in the token nibble; this writes the
/// remaining bytes: sequences of 255 followed by a final byte < 255.
#[inline]
fn write_extended_length(out: &mut Vec<u8>, mut length: usize) {
    while length >= 255 {
        out.push(255);
        length -= 255;
    }
    out.push(length as u8);
}

/// Read an extended length value, returning (extra_length, bytes_consumed).
#[inline]
fn read_extended_length(data: &[u8], pos: usize) -> Result<(usize, usize), CompressionError> {
    let mut extra = 0usize;
    let mut i = 0;
    loop {
        if pos + i >= data.len() {
            return Err(CompressionError::CorruptedData);
        }
        let byte = data[pos + i];
        extra += byte as usize;
        i += 1;
        if byte != 255 {
            break;
        }
    }
    Ok((extra, i))
}

/// Compress `input` using an LZ4-style algorithm.
///
/// Output format:
///   [original_size: u32 LE] [sequence]*
///
/// Each sequence:
///   [token: u8] [extended_literal_len?] [literal_bytes] [offset: u16 LE] [extended_match_len?]
///
/// Token byte: high nibble = literal_length (0..15), low nibble = match_length - 4 (0..15).
/// If either nibble is 15, extended length bytes follow.
///
/// The last sequence has no match part (offset + match_len are omitted).
pub fn compress(input: &[u8]) -> Vec<u8> {
    let len = input.len();

    // Header: 4-byte LE original size.
    let mut out = Vec::with_capacity(HEADER_SIZE + len);
    out.extend_from_slice(&(len as u32).to_le_bytes());

    if len == 0 {
        return out;
    }

    // For very small inputs, just emit literals.
    if len < MIN_MATCH {
        emit_last_literals(&mut out, input, 0, len);
        return out;
    }

    let mut hash_table = [0u32; HASH_TABLE_SIZE];
    let mut anchor = 0usize; // Start of un-emitted literals.
    let mut pos = 0usize;

    // We stop searching for matches MIN_MATCH bytes before the end, because
    // we need at least 4 bytes to form a hash and a match.
    let search_limit = if len > MIN_MATCH + 1 {
        len - MIN_MATCH - 1
    } else {
        0
    };

    while pos < search_limit {
        // Hash the 4 bytes at the current position.
        let h = hash4(input, pos);
        let candidate = hash_table[h] as usize;
        hash_table[h] = pos as u32;

        // Check if we found a valid match:
        // - candidate must be before current position
        // - within the sliding window
        // - first 4 bytes must match
        if candidate < pos
            && pos - candidate <= WINDOW_SIZE
            && input[candidate..candidate + MIN_MATCH] == input[pos..pos + MIN_MATCH]
        {
            // Extend the match forward.
            let ml = match_length(
                input,
                candidate + MIN_MATCH,
                pos + MIN_MATCH,
                len - pos - MIN_MATCH,
            ) + MIN_MATCH;

            // Emit the sequence: literals + match.
            let literal_len = pos - anchor;
            emit_sequence(
                &mut out,
                input,
                anchor,
                literal_len,
                (pos - candidate) as u16,
                ml,
            );

            anchor = pos + ml;
            pos = anchor;
        } else {
            pos += 1;
        }
    }

    // Emit remaining literals (the last sequence with no match).
    let remaining = len - anchor;
    if remaining > 0 {
        emit_last_literals(&mut out, input, anchor, remaining);
    }

    out
}

/// Emit a full sequence: token + extended literal len + literals + offset + extended match len.
fn emit_sequence(
    out: &mut Vec<u8>,
    input: &[u8],
    literal_start: usize,
    literal_len: usize,
    offset: u16,
    match_len: usize,
) {
    // Token: high nibble = literal_len, low nibble = match_len - MIN_MATCH.
    let lit_token = if literal_len >= 15 {
        15
    } else {
        literal_len as u8
    };
    let ml_adjusted = match_len - MIN_MATCH;
    let match_token = if ml_adjusted >= 15 {
        15
    } else {
        ml_adjusted as u8
    };
    let token = (lit_token << 4) | match_token;
    out.push(token);

    // Extended literal length.
    if literal_len >= 15 {
        write_extended_length(out, literal_len - 15);
    }

    // Literal bytes.
    out.extend_from_slice(&input[literal_start..literal_start + literal_len]);

    // Offset (u16 LE).
    out.extend_from_slice(&offset.to_le_bytes());

    // Extended match length.
    if ml_adjusted >= 15 {
        write_extended_length(out, ml_adjusted - 15);
    }
}

/// Emit the final literal-only sequence (no match part).
fn emit_last_literals(out: &mut Vec<u8>, input: &[u8], literal_start: usize, literal_len: usize) {
    let lit_token = if literal_len >= 15 {
        15
    } else {
        literal_len as u8
    };
    // Low nibble = 0 (no match).
    let token = lit_token << 4;
    out.push(token);

    if literal_len >= 15 {
        write_extended_length(out, literal_len - 15);
    }

    out.extend_from_slice(&input[literal_start..literal_start + literal_len]);
}

// ============================================================================
// Decompression
// ============================================================================

/// Decompress data that was compressed with [`compress`].
///
/// Reads the 4-byte original-size header, then decodes token sequences.
pub fn decompress(input: &[u8]) -> Result<Vec<u8>, CompressionError> {
    if input.len() < HEADER_SIZE {
        return Err(CompressionError::InvalidInput);
    }

    let original_size = u32::from_le_bytes([input[0], input[1], input[2], input[3]]) as usize;

    if original_size == 0 {
        return Ok(Vec::new());
    }

    let mut out = Vec::with_capacity(original_size);
    let mut pos = HEADER_SIZE;
    let data = input;
    let data_len = data.len();

    while pos < data_len {
        // Read token.
        let token = data[pos];
        pos += 1;
        let mut literal_len = (token >> 4) as usize;
        let match_len_base = (token & 0x0F) as usize;

        // Extended literal length.
        if literal_len == 15 {
            let (extra, consumed) = read_extended_length(data, pos)?;
            literal_len += extra;
            pos += consumed;
        }

        // Copy literals.
        if pos + literal_len > data_len {
            return Err(CompressionError::CorruptedData);
        }
        out.extend_from_slice(&data[pos..pos + literal_len]);
        pos += literal_len;

        if out.len() > original_size {
            return Err(CompressionError::BufferOverflow);
        }

        // If we've consumed all input, this was the last (literal-only) sequence.
        if pos >= data_len {
            break;
        }

        // Read offset (u16 LE).
        if pos + 2 > data_len {
            return Err(CompressionError::CorruptedData);
        }
        let offset = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;

        if offset == 0 {
            return Err(CompressionError::CorruptedData);
        }
        if offset > out.len() {
            return Err(CompressionError::CorruptedData);
        }

        // Extended match length.
        let mut match_len = match_len_base + MIN_MATCH;
        if match_len_base == 15 {
            let (extra, consumed) = read_extended_length(data, pos)?;
            match_len += extra;
            pos += consumed;
        }

        // Copy from back-reference, byte-by-byte to handle overlapping copies.
        let match_start = out.len() - offset;
        for i in 0..match_len {
            let byte = out[match_start + i];
            out.push(byte);
        }

        if out.len() > original_size {
            return Err(CompressionError::BufferOverflow);
        }
    }

    if out.len() != original_size {
        return Err(CompressionError::CorruptedData);
    }

    Ok(out)
}

// ============================================================================
// CompressedFrame
// ============================================================================

/// A compressed data frame with metadata about the original size.
///
/// Useful for storing or transmitting a single compressed payload alongside
/// its original size for quick allocation on the receiving end.
#[derive(Debug, Clone)]
pub struct CompressedFrame {
    /// Size of the original uncompressed data.
    pub original_size: u32,
    /// The compressed bytes (including the 4-byte length header).
    pub compressed_data: Vec<u8>,
}

impl CompressedFrame {
    /// Create a compressed frame from raw uncompressed data.
    pub fn from_data(data: &[u8]) -> Self {
        let compressed_data = compress(data);
        Self {
            original_size: data.len() as u32,
            compressed_data,
        }
    }

    /// Decompress the frame back to the original data.
    pub fn decompress(&self) -> Result<Vec<u8>, CompressionError> {
        decompress(&self.compressed_data)
    }

    /// Compute the compression ratio (compressed / original).
    ///
    /// A ratio < 1.0 means the data was successfully compressed.
    /// Returns `f64::INFINITY` if original size is 0.
    pub fn compression_ratio(&self) -> f64 {
        if self.original_size == 0 {
            return f64::INFINITY;
        }
        self.compressed_data.len() as f64 / self.original_size as f64
    }
}

// ============================================================================
// WireCompressor
// ============================================================================

/// Streaming-friendly compressor for wire protocol messages.
///
/// Applies compression only when the payload exceeds a configurable size
/// threshold, avoiding overhead for small messages (e.g., auth handshakes,
/// tiny queries).
#[derive(Debug, Clone)]
pub struct WireCompressor {
    /// Minimum payload size (bytes) before compression is attempted.
    threshold: usize,
}

impl WireCompressor {
    /// Create a new wire compressor with the given size threshold.
    ///
    /// Payloads smaller than `threshold` bytes are sent uncompressed.
    pub fn new(threshold: usize) -> Self {
        Self { threshold }
    }

    /// Compress `data` if it exceeds the threshold and compression is beneficial.
    ///
    /// Returns `(output, was_compressed)`. If `was_compressed` is false, `output`
    /// is the original data unchanged.
    pub fn compress_if_beneficial(&self, data: &[u8]) -> (Vec<u8>, bool) {
        if data.len() < self.threshold {
            return (data.to_vec(), false);
        }

        let compressed = compress(data);

        // Only use compressed version if it is actually smaller.
        // The compressed output includes a 4-byte header, so compare fairly.
        if compressed.len() < data.len() {
            (compressed, true)
        } else {
            (data.to_vec(), false)
        }
    }

    /// Decompress `data` if `is_compressed` is true, otherwise return it as-is.
    pub fn decompress_if_needed(
        &self,
        data: &[u8],
        is_compressed: bool,
    ) -> Result<Vec<u8>, CompressionError> {
        if is_compressed {
            decompress(data)
        } else {
            Ok(data.to_vec())
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compress_decompress_roundtrip() {
        let original = b"Hello, Nucleus wire protocol! This is a test of the compression system.";
        let compressed = compress(original);
        let decompressed = decompress(&compressed).expect("decompression should succeed");
        assert_eq!(decompressed, original);
    }

    #[test]
    fn test_empty_input() {
        let original: &[u8] = b"";
        let compressed = compress(original);
        // Should just be the 4-byte header with size 0.
        assert_eq!(compressed.len(), HEADER_SIZE);
        assert_eq!(&compressed[..4], &[0, 0, 0, 0]);

        let decompressed = decompress(&compressed).expect("decompression should succeed");
        assert_eq!(decompressed, original);
    }

    #[test]
    fn test_incompressible_data() {
        // Pseudo-random bytes — hard to compress.
        let mut data = Vec::with_capacity(1024);
        let mut state: u32 = 0xDEAD_BEEF;
        for _ in 0..1024 {
            // Simple xorshift32 PRNG.
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            data.push(state as u8);
        }

        let compressed = compress(&data);
        let decompressed = decompress(&compressed).expect("decompression should succeed");
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_highly_compressible() {
        // Repeated pattern — should compress very well.
        let pattern = b"ABCDEFGH";
        let mut data = Vec::with_capacity(pattern.len() * 500);
        for _ in 0..500 {
            data.extend_from_slice(pattern);
        }
        let original_len = data.len(); // 4000

        let compressed = compress(&data);
        let decompressed = decompress(&compressed).expect("decompression should succeed");
        assert_eq!(decompressed, data);

        // Compressed should be significantly smaller.
        assert!(
            compressed.len() < original_len / 2,
            "compressed {} should be less than half of original {}",
            compressed.len(),
            original_len,
        );
    }

    #[test]
    fn test_compression_ratio() {
        let data = "SELECT * FROM users WHERE id = 42; ".repeat(100);
        let frame = CompressedFrame::from_data(data.as_bytes());

        assert_eq!(frame.original_size, data.len() as u32);
        let ratio = frame.compression_ratio();
        assert!(ratio < 1.0, "repeated SQL should compress: ratio = {ratio}",);

        let decompressed = frame.decompress().expect("decompression should succeed");
        assert_eq!(decompressed, data.as_bytes());

        // Empty frame ratio.
        let empty_frame = CompressedFrame::from_data(b"");
        assert!(empty_frame.compression_ratio().is_infinite());
    }

    #[test]
    fn test_wire_compressor_threshold() {
        let compressor = WireCompressor::new(64);

        // Small payload: should not be compressed.
        let small = b"tiny";
        let (output, was_compressed) = compressor.compress_if_beneficial(small);
        assert!(!was_compressed);
        assert_eq!(output, small);

        // Large compressible payload: should be compressed.
        let large = "SELECT * FROM big_table WHERE col = 'value'; ".repeat(50);
        let (output, was_compressed) = compressor.compress_if_beneficial(large.as_bytes());
        assert!(
            was_compressed,
            "large repetitive payload should be compressed"
        );
        assert!(output.len() < large.len());

        // Round-trip through decompress_if_needed.
        let recovered = compressor
            .decompress_if_needed(&output, was_compressed)
            .expect("decompression should succeed");
        assert_eq!(recovered, large.as_bytes());

        // decompress_if_needed with is_compressed=false returns data as-is.
        let passthrough = compressor
            .decompress_if_needed(small, false)
            .expect("passthrough should succeed");
        assert_eq!(passthrough, small);
    }

    #[test]
    fn test_various_sizes() {
        // 1 byte
        let data_1 = vec![0xAB_u8];
        let rt_1 = decompress(&compress(&data_1)).expect("1-byte roundtrip");
        assert_eq!(rt_1, data_1);

        // 100 bytes (sequential pattern)
        let data_100: Vec<u8> = (0..100).map(|i| (i % 256) as u8).collect();
        let rt_100 = decompress(&compress(&data_100)).expect("100-byte roundtrip");
        assert_eq!(rt_100, data_100);

        // 10000 bytes (mixed content)
        let mut data_10k = Vec::with_capacity(10000);
        for i in 0..10000 {
            data_10k.push(((i * 7 + 13) % 256) as u8);
        }
        let rt_10k = decompress(&compress(&data_10k)).expect("10000-byte roundtrip");
        assert_eq!(rt_10k, data_10k);

        // 3 bytes (less than MIN_MATCH)
        let data_3 = vec![1u8, 2, 3];
        let rt_3 = decompress(&compress(&data_3)).expect("3-byte roundtrip");
        assert_eq!(rt_3, data_3);

        // Exactly MIN_MATCH bytes
        let data_min = vec![10u8, 20, 30, 40];
        let rt_min = decompress(&compress(&data_min)).expect("MIN_MATCH-byte roundtrip");
        assert_eq!(rt_min, data_min);
    }

    #[test]
    fn test_corrupt_data_detection() {
        // Too short (no header).
        assert_eq!(decompress(&[1, 2, 3]), Err(CompressionError::InvalidInput));

        // Header says 100 bytes but no compressed data follows.
        let bad_header = [100u8, 0, 0, 0];
        assert!(decompress(&bad_header).is_err());

        // Corrupt: valid header but garbled body.
        let good_data = b"The quick brown fox jumps over the lazy dog repeatedly and repeatedly.";
        let mut compressed = compress(good_data);

        // Flip some bits in the middle of the compressed payload.
        if compressed.len() > HEADER_SIZE + 4 {
            let mid = HEADER_SIZE + (compressed.len() - HEADER_SIZE) / 2;
            compressed[mid] ^= 0xFF;
            compressed[mid + 1] ^= 0xFF;
        }
        // Decompression should either fail or produce wrong data.
        // We check that it doesn't silently succeed with the original.
        match decompress(&compressed) {
            Err(_) => {} // Expected: detected corruption.
            Ok(data) => {
                // If it didn't error, the data should at least differ.
                assert_ne!(data, good_data, "corruption should be detected");
            }
        }

        // Header says 0 bytes — should succeed with empty output.
        let zero_header = [0u8, 0, 0, 0];
        assert_eq!(decompress(&zero_header).unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn test_compressed_frame_api() {
        let data = b"Nucleus database engine - wire protocol compression test payload.";
        let frame = CompressedFrame::from_data(data);

        assert_eq!(frame.original_size, data.len() as u32);
        assert!(!frame.compressed_data.is_empty());

        let recovered = frame.decompress().expect("frame decompression");
        assert_eq!(recovered, data);
    }

    #[test]
    fn test_long_literal_and_match_sequences() {
        // Create data with a long literal run (> 15 bytes) followed by a long match (> 19 bytes).
        // This exercises the extended length encoding for both literal and match lengths.

        // 300 unique bytes (long literal).
        let mut data = Vec::new();
        for i in 0..300 {
            data.push(((i * 131 + 17) % 256) as u8);
        }
        // Then 200 repetitions of the first 8 bytes (long match once hash table catches it).
        let prefix: Vec<u8> = data[0..8].to_vec();
        for _ in 0..200 {
            data.extend_from_slice(&prefix);
        }

        let compressed = compress(&data);
        let decompressed = decompress(&compressed).expect("long sequences roundtrip");
        assert_eq!(decompressed, data);
    }

    // ========================================================================
    // Property-based tests (proptest)
    // ========================================================================

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn prop_compress_decompress_roundtrip(data in proptest::collection::vec(any::<u8>(), 0..10000)) {
            let compressed = compress(&data);
            let decompressed = decompress(&compressed).expect("decompression must succeed for valid compressed data");
            prop_assert_eq!(decompressed, data);
        }

        #[test]
        fn prop_compressed_size_bounded(data in proptest::collection::vec(any::<u8>(), 0..10000)) {
            let compressed = compress(&data);
            // Worst case: 4-byte header + token byte per literal + literal bytes.
            // For each literal byte there is at most ~1 extra byte of overhead
            // (token + extended length encoding). A safe upper bound is:
            // header(4) + input_len + input_len/255 + 16 (token bytes, extended lengths).
            // Simplified: compressed size <= data.len() * 2 + 32
            let max_overhead = data.len() * 2 + 32;
            prop_assert!(
                compressed.len() <= max_overhead,
                "compressed size {} exceeds max expected {} for input size {}",
                compressed.len(), max_overhead, data.len()
            );
        }

        #[test]
        fn prop_compressed_frame_roundtrip(data in proptest::collection::vec(any::<u8>(), 0..5000)) {
            let frame = CompressedFrame::from_data(&data);
            prop_assert_eq!(frame.original_size, data.len() as u32);
            let recovered = frame.decompress().expect("frame decompression must succeed");
            prop_assert_eq!(recovered, data);
        }

        #[test]
        fn prop_wire_compressor_roundtrip(
            data in proptest::collection::vec(any::<u8>(), 0..5000),
            threshold in 0usize..512
        ) {
            let compressor = WireCompressor::new(threshold);
            let (output, was_compressed) = compressor.compress_if_beneficial(&data);
            let recovered = compressor
                .decompress_if_needed(&output, was_compressed)
                .expect("wire compressor roundtrip must succeed");
            prop_assert_eq!(recovered, data);
        }
    }

    proptest! {
        /// Compression followed by decompression is identity for all data with repeated patterns.
        #[test]
        fn prop_repeated_pattern_roundtrip(
            pattern in proptest::collection::vec(any::<u8>(), 1..32),
            reps in 1usize..100
        ) {
            let mut data = Vec::with_capacity(pattern.len() * reps);
            for _ in 0..reps {
                data.extend_from_slice(&pattern);
            }
            let compressed = compress(&data);
            let decompressed = decompress(&compressed).expect("decompression must succeed");
            prop_assert_eq!(decompressed, data);
        }

        /// Compression never increases size by more than a bounded overhead.
        #[test]
        fn prop_compression_overhead_bounded(data in proptest::collection::vec(any::<u8>(), 0..8192)) {
            let compressed = compress(&data);
            // Header is 4 bytes. Worst case each byte needs ~1 extra byte for token/length.
            // Safe bound: header(4) + data_len + data_len/255 + 20
            let upper = 4 + data.len() + data.len() / 255 + 20;
            prop_assert!(
                compressed.len() <= upper,
                "compressed len {} exceeds upper bound {} for input len {}",
                compressed.len(), upper, data.len()
            );
        }

        /// WireCompressor with threshold=0 always attempts compression.
        #[test]
        fn prop_wire_compressor_zero_threshold_roundtrip(
            data in proptest::collection::vec(any::<u8>(), 0..4096)
        ) {
            let compressor = WireCompressor::new(0);
            let (output, was_compressed) = compressor.compress_if_beneficial(&data);
            let recovered = compressor
                .decompress_if_needed(&output, was_compressed)
                .expect("roundtrip must succeed");
            prop_assert_eq!(recovered, data);
        }

        /// Below-threshold data is returned unchanged by WireCompressor.
        #[test]
        fn prop_wire_compressor_below_threshold_passthrough(
            data in proptest::collection::vec(any::<u8>(), 0..64)
        ) {
            let compressor = WireCompressor::new(64);
            let (output, was_compressed) = compressor.compress_if_beneficial(&data);
            prop_assert!(!was_compressed, "data below threshold should not be compressed");
            prop_assert_eq!(output, data);
        }

        /// CompressedFrame preserves original_size correctly.
        #[test]
        fn prop_compressed_frame_original_size(data in proptest::collection::vec(any::<u8>(), 0..4096)) {
            let frame = CompressedFrame::from_data(&data);
            prop_assert_eq!(frame.original_size as usize, data.len());
        }

        /// Double compression/decompression roundtrips (compress the compressed data, then decompress twice).
        #[test]
        fn prop_double_compress_roundtrip(data in proptest::collection::vec(any::<u8>(), 0..2048)) {
            let compressed1 = compress(&data);
            let compressed2 = compress(&compressed1);
            let decompressed2 = decompress(&compressed2).expect("outer decompression");
            prop_assert_eq!(&decompressed2, &compressed1);
            let decompressed1 = decompress(&decompressed2).expect("inner decompression");
            prop_assert_eq!(decompressed1, data);
        }
    }
}
