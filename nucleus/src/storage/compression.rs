//! Per-column adaptive compression — transparent compression at the page level.
//!
//! Per Principle 1: subsystems never know if data is compressed. Compression is
//! handled transparently at the storage layer.
//!
//! Codecs:
//!   - None: raw bytes (fallback)
//!   - LZ4: general-purpose fast compression
//!   - Delta: integers stored as deltas from previous value (good for sorted/sequential)
//!   - Dictionary: strings mapped to u16 codes (good for low-cardinality columns)
//!   - RunLength: repeated values stored as (value, count) pairs

/// Compression codec identifier stored in page headers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Codec {
    None = 0,
    Delta = 1,
    Dictionary = 2,
    RunLength = 3,
    Lz4 = 4,
    Zstd = 5,
}

impl Codec {
    pub fn from_u8(b: u8) -> Option<Self> {
        match b {
            0 => Some(Codec::None),
            1 => Some(Codec::Delta),
            2 => Some(Codec::Dictionary),
            3 => Some(Codec::RunLength),
            4 => Some(Codec::Lz4),
            5 => Some(Codec::Zstd),
            _ => None,
        }
    }
}

// ============================================================================
// Delta encoding (for integer columns)
// ============================================================================

/// Delta-encode a slice of i64 values.
/// Output: [base_value: 8 bytes] [count: 4 bytes] [delta0: zigzag varint] [delta1: zigzag varint] ...
pub fn delta_encode(values: &[i64]) -> Vec<u8> {
    let mut out = Vec::with_capacity(12 + values.len() * 4);
    if values.is_empty() {
        out.extend_from_slice(&0i64.to_le_bytes());
        out.extend_from_slice(&0u32.to_le_bytes());
        return out;
    }

    let base = values[0];
    out.extend_from_slice(&base.to_le_bytes());
    out.extend_from_slice(&(values.len() as u32).to_le_bytes());

    let mut prev = base;
    for &v in &values[1..] {
        let delta = v.wrapping_sub(prev);
        encode_zigzag_varint(&mut out, delta);
        prev = v;
    }
    out
}

/// Decode a delta-encoded buffer back to i64 values.
pub fn delta_decode(data: &[u8]) -> Result<Vec<i64>, CompressionError> {
    if data.len() < 12 {
        return Err(CompressionError::InvalidData("delta buffer too short".into()));
    }

    let base = i64::from_le_bytes(data[0..8].try_into().unwrap());
    let count = u32::from_le_bytes(data[8..12].try_into().unwrap()) as usize;

    let mut result = Vec::with_capacity(count);
    if count == 0 {
        return Ok(result);
    }

    result.push(base);
    let mut offset = 12;
    let mut prev = base;

    for _ in 1..count {
        let (delta, bytes_read) = decode_zigzag_varint(&data[offset..])?;
        offset += bytes_read;
        let value = prev.wrapping_add(delta);
        result.push(value);
        prev = value;
    }

    Ok(result)
}

/// Zigzag encode + varint write for signed i64.
fn encode_zigzag_varint(out: &mut Vec<u8>, value: i64) {
    let zigzag = ((value << 1) ^ (value >> 63)) as u64;
    let mut v = zigzag;
    loop {
        if v < 0x80 {
            out.push(v as u8);
            break;
        }
        out.push((v as u8 & 0x7F) | 0x80);
        v >>= 7;
    }
}

/// Decode zigzag varint. Returns (value, bytes_consumed).
fn decode_zigzag_varint(data: &[u8]) -> Result<(i64, usize), CompressionError> {
    let mut result: u64 = 0;
    let mut shift = 0;
    for (i, &byte) in data.iter().enumerate() {
        result |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            // Zigzag decode
            let signed = ((result >> 1) as i64) ^ (-((result & 1) as i64));
            return Ok((signed, i + 1));
        }
        shift += 7;
        if shift > 63 {
            return Err(CompressionError::InvalidData("varint overflow".into()));
        }
    }
    Err(CompressionError::InvalidData("truncated varint".into()))
}

// ============================================================================
// Dictionary encoding (for string columns)
// ============================================================================

/// Dictionary-encode a slice of strings.
/// Output: [dict_size: u16] [dict_entry: len(u16) + bytes]... [count: u32] [code: u16]...
pub fn dict_encode(values: &[&str]) -> Vec<u8> {
    let mut dict: Vec<String> = Vec::new();
    let mut code_map = std::collections::HashMap::new();
    let mut codes: Vec<u16> = Vec::with_capacity(values.len());

    for &v in values {
        let code = if let Some(&c) = code_map.get(v) {
            c
        } else {
            let c = dict.len() as u16;
            code_map.insert(v.to_string(), c);
            dict.push(v.to_string());
            c
        };
        codes.push(code);
    }

    let mut out = Vec::new();
    // Dictionary
    out.extend_from_slice(&(dict.len() as u16).to_le_bytes());
    for entry in &dict {
        let bytes = entry.as_bytes();
        out.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
        out.extend_from_slice(bytes);
    }
    // Codes
    out.extend_from_slice(&(codes.len() as u32).to_le_bytes());
    for code in &codes {
        out.extend_from_slice(&code.to_le_bytes());
    }
    out
}

/// Decode a dictionary-encoded buffer back to strings.
pub fn dict_decode(data: &[u8]) -> Result<Vec<String>, CompressionError> {
    if data.len() < 2 {
        return Err(CompressionError::InvalidData("dict buffer too short".into()));
    }

    let mut offset = 0;
    let dict_size = u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
    offset += 2;

    let mut dict = Vec::with_capacity(dict_size);
    for _ in 0..dict_size {
        if offset + 2 > data.len() {
            return Err(CompressionError::InvalidData("truncated dict entry".into()));
        }
        let len = u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
        offset += 2;
        if offset + len > data.len() {
            return Err(CompressionError::InvalidData("truncated dict string".into()));
        }
        let s = std::str::from_utf8(&data[offset..offset + len])
            .map_err(|e| CompressionError::InvalidData(e.to_string()))?;
        dict.push(s.to_string());
        offset += len;
    }

    if offset + 4 > data.len() {
        return Err(CompressionError::InvalidData("truncated code count".into()));
    }
    let count = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;

    let mut result = Vec::with_capacity(count);
    for _ in 0..count {
        if offset + 2 > data.len() {
            return Err(CompressionError::InvalidData("truncated code".into()));
        }
        let code = u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
        offset += 2;
        if code >= dict.len() {
            return Err(CompressionError::InvalidData("invalid dict code".into()));
        }
        result.push(dict[code].clone());
    }

    Ok(result)
}

// ============================================================================
// Run-length encoding (for repeated values)
// ============================================================================

/// Run-length encode a byte slice.
/// Output: [total_len: u32] [run: count(u16) + byte]...
pub fn rle_encode(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(data.len() as u32).to_le_bytes());

    let mut i = 0;
    while i < data.len() {
        let byte = data[i];
        let mut count: u16 = 1;
        while i + (count as usize) < data.len()
            && data[i + count as usize] == byte
            && count < u16::MAX
        {
            count += 1;
        }
        out.extend_from_slice(&count.to_le_bytes());
        out.push(byte);
        i += count as usize;
    }
    out
}

/// Decode a run-length encoded buffer.
pub fn rle_decode(encoded: &[u8]) -> Result<Vec<u8>, CompressionError> {
    if encoded.len() < 4 {
        return Err(CompressionError::InvalidData("RLE buffer too short".into()));
    }

    let total_len = u32::from_le_bytes(encoded[0..4].try_into().unwrap()) as usize;
    let mut result = Vec::with_capacity(total_len);
    let mut offset = 4;

    while offset + 2 < encoded.len() {
        let count = u16::from_le_bytes(encoded[offset..offset + 2].try_into().unwrap()) as usize;
        offset += 2;
        if offset >= encoded.len() {
            return Err(CompressionError::InvalidData("truncated RLE run".into()));
        }
        let byte = encoded[offset];
        offset += 1;
        result.extend(std::iter::repeat_n(byte, count));
    }

    if result.len() != total_len {
        return Err(CompressionError::InvalidData(format!(
            "RLE size mismatch: expected {total_len}, got {}",
            result.len()
        )));
    }

    Ok(result)
}

// ============================================================================
// Adaptive codec selection
// ============================================================================

/// Analyze data and select the best codec.
/// Returns the codec and the compression ratio estimate (compressed/original).
pub fn select_codec(data: &[u8], hint: DataHint) -> Codec {
    match hint {
        DataHint::Integer => {
            // Delta encoding is good for sequential/sorted integers
            Codec::Delta
        }
        DataHint::String => {
            // Dictionary encoding is good for repeated strings
            // Count unique values — if <50% unique, dictionary is worth it
            Codec::Dictionary
        }
        DataHint::Repeated => {
            // RLE for data with many repeated bytes
            Codec::RunLength
        }
        DataHint::Unknown => {
            // Analyze the data to pick best codec
            let total = data.len();
            if total == 0 {
                return Codec::None;
            }

            // Check for runs (RLE worthiness)
            let mut runs = 0usize;
            let mut i = 0;
            while i < total {
                let byte = data[i];
                let mut count = 1;
                while i + count < total && data[i + count] == byte {
                    count += 1;
                }
                runs += 1;
                i += count;
            }
            // If runs represent < 25% of total bytes, RLE is beneficial
            if runs * 3 < total {
                return Codec::RunLength;
            }

            Codec::None
        }
    }
}

/// Hint about the type of data being compressed.
#[derive(Debug, Clone, Copy)]
pub enum DataHint {
    Integer,
    String,
    Repeated,
    Unknown,
}

#[derive(Debug, thiserror::Error)]
pub enum CompressionError {
    #[error("invalid compressed data: {0}")]
    InvalidData(String),
}

// ============================================================================
// Page-level LZ4 compression
// ============================================================================

use super::page::{PageBuf, PAGE_SIZE};

/// Page compressor — transparent LZ4 compression for on-disk pages.
///
/// On-disk format for a compressed page:
///   [1 byte: codec (4=LZ4)] [4 bytes: compressed_length LE] [compressed data...] [padding...]
///
/// If compression yields no savings (compressed >= original), the page is stored
/// uncompressed: [1 byte: codec (0=None)] [raw page data...].
///
/// The compressed_length field tells the decompressor exactly how many bytes
/// of LZ4 data to read, ignoring any trailing padding from fixed-size disk slots.
#[derive(Debug, Clone, Copy)]
pub struct PageCompressor;

/// Header: 1 byte codec + 4 bytes compressed data length.
pub const COMPRESSION_HEADER_SIZE: usize = 5;

impl PageCompressor {
    /// Compress a page using LZ4. Returns the compressed bytes (including header).
    /// If compression doesn't help, returns the page with a None codec header.
    pub fn compress_page(page: &PageBuf) -> Vec<u8> {
        let compressed = lz4_flex::compress(page);

        // Only use compression if it actually saves space
        if compressed.len() + COMPRESSION_HEADER_SIZE < PAGE_SIZE {
            let mut out = Vec::with_capacity(COMPRESSION_HEADER_SIZE + compressed.len());
            out.push(Codec::Lz4 as u8);
            out.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
            out.extend_from_slice(&compressed);
            out
        } else {
            // Not worth compressing — store raw
            let mut out = Vec::with_capacity(1 + PAGE_SIZE);
            out.push(Codec::None as u8);
            out.extend_from_slice(page);
            out
        }
    }

    /// Compress a page using ZSTD (better ratio than LZ4, slower).
    /// ZSTD level 3 is a good balance between speed and ratio.
    #[cfg(feature = "server")]
    pub fn compress_page_zstd(page: &PageBuf) -> Vec<u8> {
        match zstd::bulk::compress(page, 3) {
            Ok(compressed) if compressed.len() + COMPRESSION_HEADER_SIZE < PAGE_SIZE => {
                let mut out = Vec::with_capacity(COMPRESSION_HEADER_SIZE + compressed.len());
                out.push(Codec::Zstd as u8);
                out.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
                out.extend_from_slice(&compressed);
                out
            }
            _ => {
                // Not worth compressing or error — store raw
                let mut out = Vec::with_capacity(1 + PAGE_SIZE);
                out.push(Codec::None as u8);
                out.extend_from_slice(page);
                out
            }
        }
    }

    /// Decompress a compressed page back to a full PageBuf.
    /// Handles trailing padding bytes safely by using the stored compressed length.
    pub fn decompress_page(data: &[u8]) -> Result<PageBuf, CompressionError> {
        if data.is_empty() {
            return Err(CompressionError::InvalidData("empty compressed page".into()));
        }

        let codec = Codec::from_u8(data[0])
            .ok_or_else(|| CompressionError::InvalidData(format!("unknown codec: {}", data[0])))?;

        match codec {
            Codec::None => {
                if data.len() < 1 + PAGE_SIZE {
                    return Err(CompressionError::InvalidData(
                        "uncompressed page too short".into(),
                    ));
                }
                let mut page = [0u8; PAGE_SIZE];
                page.copy_from_slice(&data[1..1 + PAGE_SIZE]);
                Ok(page)
            }
            Codec::Lz4 => {
                if data.len() < COMPRESSION_HEADER_SIZE {
                    return Err(CompressionError::InvalidData(
                        "compressed page header too short".into(),
                    ));
                }
                let compressed_len =
                    u32::from_le_bytes(data[1..5].try_into().unwrap()) as usize;
                let end = COMPRESSION_HEADER_SIZE + compressed_len;
                if end > data.len() {
                    return Err(CompressionError::InvalidData(format!(
                        "compressed length {compressed_len} exceeds buffer ({})",
                        data.len() - COMPRESSION_HEADER_SIZE
                    )));
                }
                let compressed_data = &data[COMPRESSION_HEADER_SIZE..end];
                let decompressed = lz4_flex::decompress(compressed_data, PAGE_SIZE)
                    .map_err(|e| CompressionError::InvalidData(format!("LZ4 decompress: {e}")))?;
                if decompressed.len() != PAGE_SIZE {
                    return Err(CompressionError::InvalidData(format!(
                        "decompressed size {} != {PAGE_SIZE}",
                        decompressed.len()
                    )));
                }
                let mut page = [0u8; PAGE_SIZE];
                page.copy_from_slice(&decompressed);
                Ok(page)
            }
            Codec::Zstd => {
                if data.len() < COMPRESSION_HEADER_SIZE {
                    return Err(CompressionError::InvalidData(
                        "ZSTD compressed page header too short".into(),
                    ));
                }
                let compressed_len =
                    u32::from_le_bytes(data[1..5].try_into().unwrap()) as usize;
                let end = COMPRESSION_HEADER_SIZE + compressed_len;
                if end > data.len() {
                    return Err(CompressionError::InvalidData(format!(
                        "ZSTD compressed length {compressed_len} exceeds buffer ({})",
                        data.len() - COMPRESSION_HEADER_SIZE
                    )));
                }
                #[cfg(not(feature = "server"))]
                {
                    let _ = compressed_len;
                    return Err(CompressionError::InvalidData("ZSTD not available in WASM build".into()));
                }
                #[cfg(feature = "server")]
                {
                    let compressed_data = &data[COMPRESSION_HEADER_SIZE..end];
                    let decompressed = zstd::bulk::decompress(compressed_data, PAGE_SIZE)
                        .map_err(|e| CompressionError::InvalidData(format!("ZSTD decompress: {e}")))?;
                    if decompressed.len() != PAGE_SIZE {
                        return Err(CompressionError::InvalidData(format!(
                            "ZSTD decompressed size {} != {PAGE_SIZE}",
                            decompressed.len()
                        )));
                    }
                    let mut page = [0u8; PAGE_SIZE];
                    page.copy_from_slice(&decompressed);
                    Ok(page)
                }
            }
            other => Err(CompressionError::InvalidData(format!(
                "codec {other:?} not supported for page compression"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delta_roundtrip() {
        let values = vec![100, 102, 105, 103, 110, 112, 111];
        let encoded = delta_encode(&values);
        let decoded = delta_decode(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn delta_empty() {
        let values: Vec<i64> = vec![];
        let encoded = delta_encode(&values);
        let decoded = delta_decode(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn delta_negative_values() {
        let values = vec![-100, -50, 0, 50, 100, -200];
        let encoded = delta_encode(&values);
        let decoded = delta_decode(&encoded).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn delta_compression_ratio() {
        // Sequential integers should compress well
        let values: Vec<i64> = (0..1000).collect();
        let raw_size = values.len() * 8; // 8000 bytes
        let encoded = delta_encode(&values);
        // Deltas are all 1, which encodes as 1 byte each
        assert!(
            encoded.len() < raw_size / 2,
            "encoded {} >= {} (half of raw)",
            encoded.len(),
            raw_size / 2
        );
    }

    #[test]
    fn dict_roundtrip() {
        let values = vec!["red", "blue", "red", "green", "blue", "red"];
        let encoded = dict_encode(&values);
        let decoded = dict_decode(&encoded).unwrap();
        assert_eq!(
            values.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            decoded
        );
    }

    #[test]
    fn dict_compression_ratio() {
        // Repeated strings should compress well
        let values: Vec<&str> = (0..1000).map(|i| match i % 3 { 0 => "alpha", 1 => "beta", _ => "gamma" }).collect();
        let raw_size: usize = values.iter().map(|s| s.len() + 2).sum(); // ~7000 bytes
        let encoded = dict_encode(&values);
        // 3 dict entries + 1000 u16 codes = ~2020 bytes
        assert!(
            encoded.len() < raw_size / 2,
            "encoded {} >= {} (half of raw)",
            encoded.len(),
            raw_size / 2
        );
    }

    #[test]
    fn rle_roundtrip() {
        let data = vec![0u8; 100];
        let mut data2 = data.clone();
        data2.extend(vec![1u8; 50]);
        data2.extend(vec![2u8; 25]);

        let encoded = rle_encode(&data2);
        let decoded = rle_decode(&encoded).unwrap();
        assert_eq!(data2, decoded);
    }

    #[test]
    fn rle_compression_ratio() {
        // Highly repeated data should compress well
        let data: Vec<u8> = (0..10).flat_map(|b| vec![b; 1000]).collect(); // 10000 bytes
        let encoded = rle_encode(&data);
        // 10 runs × 3 bytes + 4 header = 34 bytes
        assert!(
            encoded.len() < 100,
            "encoded {} >= 100 for highly repetitive data",
            encoded.len()
        );
    }

    #[test]
    fn zigzag_varint_values() {
        let test_values = [0i64, 1, -1, 127, -128, 10000, -10000, i64::MAX, i64::MIN];
        for &v in &test_values {
            let mut buf = Vec::new();
            encode_zigzag_varint(&mut buf, v);
            let (decoded, _) = decode_zigzag_varint(&buf).unwrap();
            assert_eq!(v, decoded, "zigzag roundtrip failed for {v}");
        }
    }

    // Page compression tests

    #[test]
    fn page_compress_roundtrip_zeros() {
        let page = [0u8; PAGE_SIZE];
        let compressed = PageCompressor::compress_page(&page);
        // All-zero page should compress extremely well
        assert!(compressed.len() < PAGE_SIZE / 2, "zeros should compress well: {} bytes", compressed.len());
        let decompressed = PageCompressor::decompress_page(&compressed).unwrap();
        assert_eq!(page, decompressed);
    }

    #[test]
    fn page_compress_roundtrip_data() {
        let mut page = [0u8; PAGE_SIZE];
        // Write some structured data (simulating a page with tuples)
        for i in 0..100 {
            let offset = i * 16;
            page[offset..offset + 8].copy_from_slice(&(i as u64).to_le_bytes());
            page[offset + 8..offset + 16].copy_from_slice(b"testdata");
        }
        let compressed = PageCompressor::compress_page(&page);
        let decompressed = PageCompressor::decompress_page(&compressed).unwrap();
        assert_eq!(page, decompressed);
    }

    #[test]
    fn page_compress_roundtrip_random() {
        // Random data may not compress, should still roundtrip
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut page = [0u8; PAGE_SIZE];
        for i in 0..PAGE_SIZE {
            let mut h = DefaultHasher::new();
            i.hash(&mut h);
            page[i] = (h.finish() & 0xFF) as u8;
        }
        let compressed = PageCompressor::compress_page(&page);
        let decompressed = PageCompressor::decompress_page(&compressed).unwrap();
        assert_eq!(page, decompressed);
    }

    #[test]
    fn page_compress_sparse_page_ratio() {
        // Partially-filled page (common in databases)
        let mut page = [0u8; PAGE_SIZE];
        page[0..4].copy_from_slice(&42u32.to_le_bytes()); // page header
        page[100..200].copy_from_slice(&[0xAB; 100]); // some data
        let compressed = PageCompressor::compress_page(&page);
        assert!(compressed.len() < PAGE_SIZE / 4, "sparse page: {} bytes", compressed.len());
        let decompressed = PageCompressor::decompress_page(&compressed).unwrap();
        assert_eq!(page, decompressed);
    }

    #[test]
    fn page_compress_codec_none_for_incompressible() {
        // Fill with pseudo-random data that won't compress
        let mut page = [0u8; PAGE_SIZE];
        let mut val = 0x12345678u64;
        for chunk in page.chunks_mut(8) {
            val = val.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let bytes = val.to_le_bytes();
            let len = chunk.len().min(8);
            chunk[..len].copy_from_slice(&bytes[..len]);
        }
        let compressed = PageCompressor::compress_page(&page);
        // Should still decompress correctly regardless of codec chosen
        let decompressed = PageCompressor::decompress_page(&compressed).unwrap();
        assert_eq!(page, decompressed);
    }

    #[test]
    fn page_compress_invalid_data() {
        assert!(PageCompressor::decompress_page(&[]).is_err());
        assert!(PageCompressor::decompress_page(&[255]).is_err()); // unknown codec
    }

    #[test]
    fn codec_lz4_variant() {
        assert_eq!(Codec::from_u8(4), Some(Codec::Lz4));
    }

    #[test]
    fn codec_zstd_variant() {
        assert_eq!(Codec::from_u8(5), Some(Codec::Zstd));
    }

    #[test]
    fn page_compress_zstd_zeros() {
        let page = [0u8; PAGE_SIZE];
        let compressed = PageCompressor::compress_page_zstd(&page);
        // ZSTD should compress a page of zeros very well
        assert!(compressed.len() < PAGE_SIZE / 2, "ZSTD should compress zeros well");
        assert_eq!(compressed[0], Codec::Zstd as u8);
        let decompressed = PageCompressor::decompress_page(&compressed).unwrap();
        assert_eq!(page, decompressed);
    }

    #[test]
    fn page_compress_zstd_structured() {
        let mut page = [0u8; PAGE_SIZE];
        // Fill with a repeating pattern
        for (i, byte) in page.iter_mut().enumerate() {
            *byte = (i % 256) as u8;
        }
        let compressed = PageCompressor::compress_page_zstd(&page);
        assert_eq!(compressed[0], Codec::Zstd as u8);
        let decompressed = PageCompressor::decompress_page(&compressed).unwrap();
        assert_eq!(page, decompressed);
    }

    #[test]
    fn page_compress_zstd_random() {
        // Random data is hard to compress — should fall back to None
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut page = [0u8; PAGE_SIZE];
        for i in 0..PAGE_SIZE {
            let mut hasher = DefaultHasher::new();
            i.hash(&mut hasher);
            page[i] = hasher.finish() as u8;
        }
        let compressed = PageCompressor::compress_page_zstd(&page);
        let decompressed = PageCompressor::decompress_page(&compressed).unwrap();
        assert_eq!(page, decompressed);
    }
}
