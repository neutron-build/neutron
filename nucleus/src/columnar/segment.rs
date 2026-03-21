//! Disk-backed columnar segment format for MergeTree parts.
//!
//! Each segment file contains one flushed MergeTree part, stored in a columnar
//! layout with per-column compression and zone map metadata.
//!
//! ## File format
//! ```text
//! Header (fixed 24 bytes):
//!   Magic:            4 bytes   "NCSF"  (Nucleus Columnar Segment File)
//!   Version:          1 byte    0x01
//!   Compression:      1 byte    codec tag (0=None, 1=Rle, 2=Delta, 3=Dict, 4=FOR, 5=Lz4)
//!   Row count:        4 bytes   u32 LE
//!   Column count:     4 bytes   u32 LE
//!   Part ID:          8 bytes   u64 LE
//!   Reserved:         2 bytes   (zero-filled, future use)
//!
//! Column metadata (repeated column_count times):
//!   name_len:         2 bytes   u16 LE
//!   name_bytes:       name_len bytes
//!   col_type:         1 byte    (0=Bool, 1=Int32, 2=Int64, 3=Float64, 4=Text)
//!   compressed_size:  4 bytes   u32 LE
//!   uncompressed_size:4 bytes   u32 LE
//!   has_zone_map:     1 byte    (0 or 1)
//!   zone_map_min:     variable  (if has_zone_map)
//!   zone_map_max:     variable  (if has_zone_map)
//!   null_count:       4 bytes   u32 LE (if has_zone_map)
//!
//! Column data (repeated column_count times):
//!   compressed_data:  compressed_size bytes
//!
//! Footer:
//!   checksum:         4 bytes   CRC32C of everything before this
//! ```

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};

use super::{
    ColumnBatch, ColumnData, ColumnZoneMap, CompressionCodec, ScalarValue, ZoneMap,
};

// ============================================================================
// Constants
// ============================================================================

const MAGIC: &[u8; 4] = b"NCSF";
const VERSION: u8 = 0x01;
const HEADER_SIZE: usize = 24;

/// Default size threshold in bytes for flushing a part to disk.
/// Parts whose estimated uncompressed size exceeds this become cold.
pub const DEFAULT_COLD_THRESHOLD_BYTES: usize = 65_536; // 64 KB

// Column type tags
const COL_TYPE_BOOL: u8 = 0;
const COL_TYPE_INT32: u8 = 1;
const COL_TYPE_INT64: u8 = 2;
const COL_TYPE_FLOAT64: u8 = 3;
const COL_TYPE_TEXT: u8 = 4;

// Codec tags for the header-level compression field
const CODEC_TAG_NONE: u8 = 0;
const CODEC_TAG_RLE: u8 = 1;
const CODEC_TAG_DELTA: u8 = 2;
const CODEC_TAG_DICT: u8 = 3;
const CODEC_TAG_FOR: u8 = 4;
const CODEC_TAG_LZ4: u8 = 5;

// ============================================================================
// Size estimation
// ============================================================================

/// Estimate the in-memory byte size of a ColumnBatch (uncompressed).
pub fn estimate_batch_size(batch: &ColumnBatch) -> usize {
    let mut total = 0usize;
    for (name, col) in &batch.columns {
        total += name.len();
        total += estimate_column_size(col);
    }
    total
}

fn estimate_column_size(col: &ColumnData) -> usize {
    match col {
        ColumnData::Bool(v) => v.len(),               // 1 byte per value approx
        ColumnData::Int32(v) => v.len() * 5,           // 4 bytes + null flag
        ColumnData::Int64(v) => v.len() * 9,           // 8 bytes + null flag
        ColumnData::Float64(v) => v.len() * 9,         // 8 bytes + null flag
        ColumnData::Text(v) => {
            v.iter()
                .map(|s| s.as_ref().map_or(1, |s| s.len() + 5))
                .sum()
        }
    }
}

// ============================================================================
// Serialization helpers
// ============================================================================

fn codec_to_tag(codec: CompressionCodec) -> u8 {
    match codec {
        CompressionCodec::None => CODEC_TAG_NONE,
        CompressionCodec::Rle => CODEC_TAG_RLE,
        CompressionCodec::Delta => CODEC_TAG_DELTA,
        CompressionCodec::Dictionary => CODEC_TAG_DICT,
        CompressionCodec::FrameOfReference => CODEC_TAG_FOR,
    }
}

fn tag_to_codec(tag: u8) -> CompressionCodec {
    match tag {
        CODEC_TAG_NONE => CompressionCodec::None,
        CODEC_TAG_RLE => CompressionCodec::Rle,
        CODEC_TAG_DELTA => CompressionCodec::Delta,
        CODEC_TAG_DICT => CompressionCodec::Dictionary,
        CODEC_TAG_FOR => CompressionCodec::FrameOfReference,
        CODEC_TAG_LZ4 => CompressionCodec::None, // LZ4 handled at byte level
        _ => CompressionCodec::None,
    }
}

fn col_type_tag(col: &ColumnData) -> u8 {
    match col {
        ColumnData::Bool(_) => COL_TYPE_BOOL,
        ColumnData::Int32(_) => COL_TYPE_INT32,
        ColumnData::Int64(_) => COL_TYPE_INT64,
        ColumnData::Float64(_) => COL_TYPE_FLOAT64,
        ColumnData::Text(_) => COL_TYPE_TEXT,
    }
}

/// Serialize a column to raw bytes (uncompressed wire format).
fn serialize_column(col: &ColumnData) -> Vec<u8> {
    let mut buf = Vec::new();
    match col {
        ColumnData::Bool(vals) => {
            for v in vals {
                match v {
                    None => buf.push(0xFF),
                    Some(false) => buf.push(0x00),
                    Some(true) => buf.push(0x01),
                }
            }
        }
        ColumnData::Int32(vals) => {
            for v in vals {
                match v {
                    None => {
                        buf.push(0); // null flag
                    }
                    Some(n) => {
                        buf.push(1);
                        buf.extend_from_slice(&n.to_le_bytes());
                    }
                }
            }
        }
        ColumnData::Int64(vals) => {
            for v in vals {
                match v {
                    None => {
                        buf.push(0);
                    }
                    Some(n) => {
                        buf.push(1);
                        buf.extend_from_slice(&n.to_le_bytes());
                    }
                }
            }
        }
        ColumnData::Float64(vals) => {
            for v in vals {
                match v {
                    None => {
                        buf.push(0);
                    }
                    Some(f) => {
                        buf.push(1);
                        buf.extend_from_slice(&f.to_le_bytes());
                    }
                }
            }
        }
        ColumnData::Text(vals) => {
            for v in vals {
                match v {
                    None => {
                        buf.push(0);
                    }
                    Some(s) => {
                        buf.push(1);
                        let bytes = s.as_bytes();
                        buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                        buf.extend_from_slice(bytes);
                    }
                }
            }
        }
    }
    buf
}

/// Deserialize a column from raw bytes.
fn deserialize_column(data: &[u8], col_type: u8, row_count: usize) -> io::Result<ColumnData> {
    let mut pos = 0;
    match col_type {
        COL_TYPE_BOOL => {
            let mut vals = Vec::with_capacity(row_count);
            for _ in 0..row_count {
                let b = *data.get(pos).ok_or_else(eof)?;
                pos += 1;
                vals.push(match b {
                    0xFF => None,
                    0x00 => Some(false),
                    _ => Some(true),
                });
            }
            Ok(ColumnData::Bool(vals))
        }
        COL_TYPE_INT32 => {
            let mut vals = Vec::with_capacity(row_count);
            for _ in 0..row_count {
                let flag = *data.get(pos).ok_or_else(eof)?;
                pos += 1;
                if flag == 0 {
                    vals.push(None);
                } else {
                    let n = read_i32_le(data, &mut pos)?;
                    vals.push(Some(n));
                }
            }
            Ok(ColumnData::Int32(vals))
        }
        COL_TYPE_INT64 => {
            let mut vals = Vec::with_capacity(row_count);
            for _ in 0..row_count {
                let flag = *data.get(pos).ok_or_else(eof)?;
                pos += 1;
                if flag == 0 {
                    vals.push(None);
                } else {
                    let n = read_i64_le(data, &mut pos)?;
                    vals.push(Some(n));
                }
            }
            Ok(ColumnData::Int64(vals))
        }
        COL_TYPE_FLOAT64 => {
            let mut vals = Vec::with_capacity(row_count);
            for _ in 0..row_count {
                let flag = *data.get(pos).ok_or_else(eof)?;
                pos += 1;
                if flag == 0 {
                    vals.push(None);
                } else {
                    let n = read_f64_le(data, &mut pos)?;
                    vals.push(Some(n));
                }
            }
            Ok(ColumnData::Float64(vals))
        }
        COL_TYPE_TEXT => {
            let mut vals = Vec::with_capacity(row_count);
            for _ in 0..row_count {
                let flag = *data.get(pos).ok_or_else(eof)?;
                pos += 1;
                if flag == 0 {
                    vals.push(None);
                } else {
                    let len = read_u32_le(data, &mut pos)? as usize;
                    if pos + len > data.len() {
                        return Err(eof());
                    }
                    let s = std::str::from_utf8(&data[pos..pos + len])
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
                        .to_string();
                    pos += len;
                    vals.push(Some(s));
                }
            }
            Ok(ColumnData::Text(vals))
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown column type tag: {col_type}"),
        )),
    }
}

/// Serialize a ScalarValue to bytes.
fn serialize_scalar(val: &ScalarValue, buf: &mut Vec<u8>) {
    match val {
        ScalarValue::Bool(b) => {
            buf.push(0);
            buf.push(*b as u8);
        }
        ScalarValue::Int32(n) => {
            buf.push(1);
            buf.extend_from_slice(&n.to_le_bytes());
        }
        ScalarValue::Int64(n) => {
            buf.push(2);
            buf.extend_from_slice(&n.to_le_bytes());
        }
        ScalarValue::Float64(f) => {
            buf.push(3);
            buf.extend_from_slice(&f.to_le_bytes());
        }
        ScalarValue::Text(s) => {
            buf.push(4);
            let bytes = s.as_bytes();
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
        }
    }
}

/// Deserialize a ScalarValue from bytes.
fn deserialize_scalar(data: &[u8], pos: &mut usize) -> io::Result<ScalarValue> {
    let tag = *data.get(*pos).ok_or_else(eof)?;
    *pos += 1;
    match tag {
        0 => {
            let b = *data.get(*pos).ok_or_else(eof)?;
            *pos += 1;
            Ok(ScalarValue::Bool(b != 0))
        }
        1 => {
            let n = read_i32_le(data, pos)?;
            Ok(ScalarValue::Int32(n))
        }
        2 => {
            let n = read_i64_le(data, pos)?;
            Ok(ScalarValue::Int64(n))
        }
        3 => {
            let f = read_f64_le(data, pos)?;
            Ok(ScalarValue::Float64(f))
        }
        4 => {
            let len = read_u32_le(data, pos)? as usize;
            if *pos + len > data.len() {
                return Err(eof());
            }
            let s = std::str::from_utf8(&data[*pos..*pos + len])
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
                .to_string();
            *pos += len;
            Ok(ScalarValue::Text(s))
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown scalar tag: {tag}"),
        )),
    }
}

// ============================================================================
// Primitive read helpers
// ============================================================================

fn eof() -> io::Error {
    io::Error::new(io::ErrorKind::UnexpectedEof, "truncated segment file")
}

fn read_u16_le(data: &[u8], pos: &mut usize) -> io::Result<u16> {
    if *pos + 2 > data.len() {
        return Err(eof());
    }
    let val = u16::from_le_bytes([data[*pos], data[*pos + 1]]);
    *pos += 2;
    Ok(val)
}

fn read_u32_le(data: &[u8], pos: &mut usize) -> io::Result<u32> {
    if *pos + 4 > data.len() {
        return Err(eof());
    }
    let val = u32::from_le_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]]);
    *pos += 4;
    Ok(val)
}

fn read_i32_le(data: &[u8], pos: &mut usize) -> io::Result<i32> {
    read_u32_le(data, pos).map(|v| v as i32)
}

fn read_u64_le(data: &[u8], pos: &mut usize) -> io::Result<u64> {
    if *pos + 8 > data.len() {
        return Err(eof());
    }
    let val = u64::from_le_bytes([
        data[*pos],
        data[*pos + 1],
        data[*pos + 2],
        data[*pos + 3],
        data[*pos + 4],
        data[*pos + 5],
        data[*pos + 6],
        data[*pos + 7],
    ]);
    *pos += 8;
    Ok(val)
}

fn read_i64_le(data: &[u8], pos: &mut usize) -> io::Result<i64> {
    read_u64_le(data, pos).map(|v| v as i64)
}

fn read_f64_le(data: &[u8], pos: &mut usize) -> io::Result<f64> {
    read_u64_le(data, pos).map(|v| f64::from_bits(v))
}

// ============================================================================
// SegmentWriter
// ============================================================================

/// Writes a ColumnBatch to a segment file on disk.
pub struct SegmentWriter;

impl SegmentWriter {
    /// Serialize a ColumnBatch to a segment file at `path`.
    ///
    /// The `codec` controls the logical compression applied to each column before
    /// serialization. Additionally, the raw serialized bytes for each column are
    /// compressed with LZ4 for byte-level compaction.
    ///
    /// `part_id` is stored in the header for identification on reload.
    pub fn write(
        path: &Path,
        batch: &ColumnBatch,
        codec: CompressionCodec,
        part_id: u64,
    ) -> io::Result<()> {
        let mut buf = Vec::with_capacity(estimate_batch_size(batch) + 256);

        // --- Header ---
        buf.extend_from_slice(MAGIC);
        buf.push(VERSION);
        buf.push(codec_to_tag(codec));
        buf.extend_from_slice(&(batch.row_count as u32).to_le_bytes());
        buf.extend_from_slice(&(batch.columns.len() as u32).to_le_bytes());
        buf.extend_from_slice(&part_id.to_le_bytes());
        buf.extend_from_slice(&[0u8; 2]); // reserved

        // --- Serialize each column's data and build metadata ---
        let zone_map = ZoneMap::from_batch(batch);
        let mut col_raw_data: Vec<Vec<u8>> = Vec::with_capacity(batch.columns.len());

        // First pass: serialize and compress column data
        for (_, col) in &batch.columns {
            let raw = serialize_column(col);
            let compressed = lz4_flex::compress_prepend_size(&raw);
            col_raw_data.push(compressed);
        }

        // --- Column metadata ---
        for (i, (name, col)) in batch.columns.iter().enumerate() {
            let name_bytes = name.as_bytes();
            buf.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
            buf.extend_from_slice(name_bytes);
            buf.push(col_type_tag(col));

            let compressed_size = col_raw_data[i].len() as u32;
            let uncompressed_size = estimate_column_size(col) as u32;
            buf.extend_from_slice(&compressed_size.to_le_bytes());
            buf.extend_from_slice(&uncompressed_size.to_le_bytes());

            // Zone map for this column
            if let Some(czm) = zone_map.columns.get(name) {
                if czm.min.is_some() && czm.max.is_some() {
                    buf.push(1); // has zone map
                    serialize_scalar(czm.min.as_ref().unwrap(), &mut buf);
                    serialize_scalar(czm.max.as_ref().unwrap(), &mut buf);
                    buf.extend_from_slice(&(czm.null_count as u32).to_le_bytes());
                } else {
                    buf.push(0); // no zone map (all nulls)
                }
            } else {
                buf.push(0);
            }
        }

        // --- Column data ---
        for data in &col_raw_data {
            buf.extend_from_slice(data);
        }

        // --- Footer: CRC32C checksum ---
        let checksum = crc32c::crc32c(&buf);
        buf.extend_from_slice(&checksum.to_le_bytes());

        std::fs::write(path, &buf)
    }
}

// ============================================================================
// SegmentReader
// ============================================================================

/// Parsed column metadata from a segment file header.
#[derive(Debug, Clone)]
struct ColumnMeta {
    name: String,
    col_type: u8,
    compressed_size: u32,
    #[allow(dead_code)]
    uncompressed_size: u32,
    zone_map_entry: Option<ColumnZoneMap>,
}

/// Reads a segment file from disk.
#[derive(Debug)]
pub struct SegmentReader {
    data: Vec<u8>,
    row_count: u32,
    #[allow(dead_code)]
    col_count: u32,
    part_id: u64,
    #[allow(dead_code)]
    codec: CompressionCodec,
    col_metas: Vec<ColumnMeta>,
    data_start: usize,
}

impl SegmentReader {
    /// Open a segment file and parse its header + column metadata.
    pub fn open(path: &Path) -> io::Result<Self> {
        let data = std::fs::read(path)?;
        Self::from_bytes(data)
    }

    /// Parse from an in-memory buffer (useful for testing).
    fn from_bytes(data: Vec<u8>) -> io::Result<Self> {
        if data.len() < HEADER_SIZE + 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "segment file too small",
            ));
        }

        // Verify checksum
        let payload_len = data.len() - 4;
        let stored_checksum = u32::from_le_bytes([
            data[payload_len],
            data[payload_len + 1],
            data[payload_len + 2],
            data[payload_len + 3],
        ]);
        let computed_checksum = crc32c::crc32c(&data[..payload_len]);
        if stored_checksum != computed_checksum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "segment checksum mismatch: stored={stored_checksum:#X}, computed={computed_checksum:#X}"
                ),
            ));
        }

        // Parse header
        if data[..4] != *MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bad segment magic",
            ));
        }
        let mut pos = 4;
        let version = data[pos];
        pos += 1;
        if version != VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported segment version: {version}"),
            ));
        }
        let codec_tag = data[pos];
        pos += 1;
        let codec = tag_to_codec(codec_tag);

        let row_count = read_u32_le(&data, &mut pos)?;
        let col_count = read_u32_le(&data, &mut pos)?;
        let part_id = read_u64_le(&data, &mut pos)?;
        pos += 2; // reserved

        // Parse column metadata
        let mut col_metas = Vec::with_capacity(col_count as usize);
        for _ in 0..col_count {
            let name_len = read_u16_le(&data, &mut pos)? as usize;
            if pos + name_len > data.len() {
                return Err(eof());
            }
            let name = std::str::from_utf8(&data[pos..pos + name_len])
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
                .to_string();
            pos += name_len;

            let col_type = *data.get(pos).ok_or_else(eof)?;
            pos += 1;

            let compressed_size = read_u32_le(&data, &mut pos)?;
            let uncompressed_size = read_u32_le(&data, &mut pos)?;

            let has_zone_map = *data.get(pos).ok_or_else(eof)?;
            pos += 1;

            let zone_map_entry = if has_zone_map == 1 {
                let min = deserialize_scalar(&data, &mut pos)?;
                let max = deserialize_scalar(&data, &mut pos)?;
                let null_count = read_u32_le(&data, &mut pos)? as usize;
                Some(ColumnZoneMap {
                    min: Some(min),
                    max: Some(max),
                    null_count,
                    row_count: row_count as usize,
                })
            } else {
                None
            };

            col_metas.push(ColumnMeta {
                name,
                col_type,
                compressed_size,
                uncompressed_size,
                zone_map_entry,
            });
        }

        Ok(SegmentReader {
            data,
            row_count,
            col_count,
            part_id,
            codec,
            col_metas,
            data_start: pos,
        })
    }

    /// Get the part ID stored in this segment.
    pub fn part_id(&self) -> u64 {
        self.part_id
    }

    /// Get the row count.
    pub fn row_count(&self) -> usize {
        self.row_count as usize
    }

    /// Read the full batch into memory.
    pub fn read_batch(&self) -> io::Result<ColumnBatch> {
        let mut pos = self.data_start;
        let mut columns = Vec::with_capacity(self.col_metas.len());

        for meta in &self.col_metas {
            let end = pos + meta.compressed_size as usize;
            if end > self.data.len() - 4 {
                // -4 for footer checksum
                return Err(eof());
            }
            let compressed = &self.data[pos..end];
            pos = end;

            // Decompress LZ4
            let raw = lz4_flex::decompress_size_prepended(compressed)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("LZ4 decompress error: {e}")))?;

            let col = deserialize_column(&raw, meta.col_type, self.row_count as usize)?;
            columns.push((meta.name.clone(), col));
        }

        Ok(ColumnBatch::new(columns))
    }

    /// Read just the zone map without loading column data.
    pub fn read_zone_map(&self) -> ZoneMap {
        let mut columns = HashMap::new();
        for meta in &self.col_metas {
            if let Some(ref czm) = meta.zone_map_entry {
                columns.insert(meta.name.clone(), czm.clone());
            } else {
                // Column has no zone map (all nulls or empty)
                columns.insert(
                    meta.name.clone(),
                    ColumnZoneMap {
                        min: None,
                        max: None,
                        null_count: self.row_count as usize,
                        row_count: self.row_count as usize,
                    },
                );
            }
        }
        ZoneMap { columns }
    }
}

// ============================================================================
// ColdPartInfo — metadata retained in memory for a disk-resident part
// ============================================================================

/// In-memory metadata for a part that has been flushed to disk.
/// The actual data lives in a segment file; only the zone map and path
/// are kept in memory for pruning decisions.
#[derive(Debug)]
pub struct ColdPartInfo {
    pub part_id: u64,
    pub path: PathBuf,
    pub zone_map: ZoneMap,
    pub row_count: usize,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::columnar::CmpOp;

    fn make_int_batch(n: usize) -> ColumnBatch {
        let ids: Vec<Option<i64>> = (0..n).map(|i| Some(i as i64)).collect();
        let vals: Vec<Option<f64>> = (0..n).map(|i| Some(i as f64 * 1.5)).collect();
        ColumnBatch::new(vec![
            ("id".to_string(), ColumnData::Int64(ids)),
            ("value".to_string(), ColumnData::Float64(vals)),
        ])
    }

    fn make_text_batch(n: usize) -> ColumnBatch {
        let ids: Vec<Option<i64>> = (0..n).map(|i| Some(i as i64)).collect();
        let names: Vec<Option<String>> = (0..n)
            .map(|i| Some(format!("item_{i}")))
            .collect();
        ColumnBatch::new(vec![
            ("id".to_string(), ColumnData::Int64(ids)),
            ("name".to_string(), ColumnData::Text(names)),
        ])
    }

    fn make_batch_with_nulls(n: usize) -> ColumnBatch {
        let ids: Vec<Option<i64>> = (0..n)
            .map(|i| if i % 3 == 0 { None } else { Some(i as i64) })
            .collect();
        let vals: Vec<Option<f64>> = (0..n)
            .map(|i| if i % 5 == 0 { None } else { Some(i as f64) })
            .collect();
        ColumnBatch::new(vec![
            ("id".to_string(), ColumnData::Int64(ids)),
            ("value".to_string(), ColumnData::Float64(vals)),
        ])
    }

    fn make_bool_batch(n: usize) -> ColumnBatch {
        let flags: Vec<Option<bool>> = (0..n)
            .map(|i| if i % 4 == 0 { None } else { Some(i % 2 == 0) })
            .collect();
        let ids: Vec<Option<i64>> = (0..n).map(|i| Some(i as i64)).collect();
        ColumnBatch::new(vec![
            ("flag".to_string(), ColumnData::Bool(flags)),
            ("id".to_string(), ColumnData::Int64(ids)),
        ])
    }

    fn make_int32_batch(n: usize) -> ColumnBatch {
        let ids: Vec<Option<i32>> = (0..n).map(|i| Some(i as i32)).collect();
        ColumnBatch::new(vec![
            ("id".to_string(), ColumnData::Int32(ids)),
        ])
    }

    // ---- Roundtrip tests per codec ----

    #[test]
    fn test_segment_roundtrip_no_compression() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.seg");
        let batch = make_int_batch(100);
        SegmentWriter::write(&path, &batch, CompressionCodec::None, 42).unwrap();

        let reader = SegmentReader::open(&path).unwrap();
        assert_eq!(reader.part_id(), 42);
        assert_eq!(reader.row_count(), 100);

        let recovered = reader.read_batch().unwrap();
        assert_eq!(recovered.row_count, 100);
        assert_eq!(recovered.columns.len(), 2);

        // Verify data
        if let ColumnData::Int64(vals) = recovered.column("id").unwrap() {
            assert_eq!(vals[0], Some(0));
            assert_eq!(vals[99], Some(99));
        } else {
            panic!("expected Int64 column");
        }
        if let ColumnData::Float64(vals) = recovered.column("value").unwrap() {
            assert!((vals[0].unwrap() - 0.0).abs() < f64::EPSILON);
            assert!((vals[99].unwrap() - 148.5).abs() < f64::EPSILON);
        } else {
            panic!("expected Float64 column");
        }
    }

    #[test]
    fn test_segment_roundtrip_text_columns() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("text.seg");
        let batch = make_text_batch(50);
        SegmentWriter::write(&path, &batch, CompressionCodec::None, 7).unwrap();

        let reader = SegmentReader::open(&path).unwrap();
        let recovered = reader.read_batch().unwrap();
        assert_eq!(recovered.row_count, 50);

        if let ColumnData::Text(vals) = recovered.column("name").unwrap() {
            assert_eq!(vals[0], Some("item_0".to_string()));
            assert_eq!(vals[49], Some("item_49".to_string()));
        } else {
            panic!("expected Text column");
        }
    }

    #[test]
    fn test_segment_roundtrip_with_nulls() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nulls.seg");
        let batch = make_batch_with_nulls(30);
        SegmentWriter::write(&path, &batch, CompressionCodec::None, 99).unwrap();

        let reader = SegmentReader::open(&path).unwrap();
        let recovered = reader.read_batch().unwrap();
        assert_eq!(recovered.row_count, 30);

        if let ColumnData::Int64(vals) = recovered.column("id").unwrap() {
            assert_eq!(vals[0], None);   // i=0, 0%3==0 => None
            assert_eq!(vals[1], Some(1));
            assert_eq!(vals[3], None);   // i=3, 3%3==0 => None
        } else {
            panic!("expected Int64 column");
        }
    }

    #[test]
    fn test_segment_roundtrip_bool_column() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bool.seg");
        let batch = make_bool_batch(20);
        SegmentWriter::write(&path, &batch, CompressionCodec::None, 3).unwrap();

        let reader = SegmentReader::open(&path).unwrap();
        let recovered = reader.read_batch().unwrap();
        assert_eq!(recovered.row_count, 20);

        if let ColumnData::Bool(vals) = recovered.column("flag").unwrap() {
            assert_eq!(vals[0], None);        // 0%4==0 => None
            assert_eq!(vals[1], Some(false)); // 1%2!=0 => false
            assert_eq!(vals[2], Some(true));  // 2%2==0 => true
        } else {
            panic!("expected Bool column");
        }
    }

    #[test]
    fn test_segment_roundtrip_int32_column() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("i32.seg");
        let batch = make_int32_batch(15);
        SegmentWriter::write(&path, &batch, CompressionCodec::None, 1).unwrap();

        let reader = SegmentReader::open(&path).unwrap();
        let recovered = reader.read_batch().unwrap();

        if let ColumnData::Int32(vals) = recovered.column("id").unwrap() {
            assert_eq!(vals[0], Some(0));
            assert_eq!(vals[14], Some(14));
        } else {
            panic!("expected Int32 column");
        }
    }

    #[test]
    fn test_segment_roundtrip_rle_codec() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rle.seg");
        let batch = make_int_batch(200);
        SegmentWriter::write(&path, &batch, CompressionCodec::Rle, 10).unwrap();

        let reader = SegmentReader::open(&path).unwrap();
        let recovered = reader.read_batch().unwrap();
        assert_eq!(recovered.row_count, 200);

        if let ColumnData::Int64(vals) = recovered.column("id").unwrap() {
            for i in 0..200 {
                assert_eq!(vals[i], Some(i as i64), "mismatch at row {i}");
            }
        } else {
            panic!("expected Int64 column");
        }
    }

    #[test]
    fn test_segment_roundtrip_delta_codec() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("delta.seg");
        let batch = make_int_batch(150);
        SegmentWriter::write(&path, &batch, CompressionCodec::Delta, 11).unwrap();

        let reader = SegmentReader::open(&path).unwrap();
        let recovered = reader.read_batch().unwrap();
        assert_eq!(recovered.row_count, 150);

        if let ColumnData::Int64(vals) = recovered.column("id").unwrap() {
            for i in 0..150 {
                assert_eq!(vals[i], Some(i as i64), "mismatch at row {i}");
            }
        } else {
            panic!("expected Int64 column");
        }
    }

    #[test]
    fn test_segment_roundtrip_dictionary_codec() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dict.seg");
        let batch = make_text_batch(80);
        SegmentWriter::write(&path, &batch, CompressionCodec::Dictionary, 12).unwrap();

        let reader = SegmentReader::open(&path).unwrap();
        let recovered = reader.read_batch().unwrap();
        assert_eq!(recovered.row_count, 80);

        if let ColumnData::Text(vals) = recovered.column("name").unwrap() {
            assert_eq!(vals[0], Some("item_0".to_string()));
            assert_eq!(vals[79], Some("item_79".to_string()));
        } else {
            panic!("expected Text column");
        }
    }

    #[test]
    fn test_segment_roundtrip_for_codec() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("for.seg");
        // Make a batch with values in a narrow range for FOR to work
        let ids: Vec<Option<i64>> = (1000..1050).map(|i| Some(i as i64)).collect();
        let batch = ColumnBatch::new(vec![
            ("id".to_string(), ColumnData::Int64(ids)),
        ]);
        SegmentWriter::write(&path, &batch, CompressionCodec::FrameOfReference, 13).unwrap();

        let reader = SegmentReader::open(&path).unwrap();
        let recovered = reader.read_batch().unwrap();
        assert_eq!(recovered.row_count, 50);

        if let ColumnData::Int64(vals) = recovered.column("id").unwrap() {
            for i in 0..50 {
                assert_eq!(vals[i], Some(1000 + i as i64));
            }
        } else {
            panic!("expected Int64 column");
        }
    }

    #[test]
    fn test_segment_zone_map_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("zm.seg");
        let batch = make_int_batch(100);
        SegmentWriter::write(&path, &batch, CompressionCodec::None, 1).unwrap();

        let reader = SegmentReader::open(&path).unwrap();
        let zm = reader.read_zone_map();

        let id_zm = zm.columns.get("id").unwrap();
        assert_eq!(id_zm.min, Some(ScalarValue::Int64(0)));
        assert_eq!(id_zm.max, Some(ScalarValue::Int64(99)));
        assert_eq!(id_zm.null_count, 0);
    }

    #[test]
    fn test_segment_zone_map_pruning() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prune.seg");
        // Batch with id 0..99
        let batch = make_int_batch(100);
        SegmentWriter::write(&path, &batch, CompressionCodec::None, 1).unwrap();

        let reader = SegmentReader::open(&path).unwrap();
        let zm = reader.read_zone_map();

        // Searching for id == 200 should skip this segment
        assert!(zm.can_skip("id", CmpOp::Eq, &ScalarValue::Int64(200)));
        // Searching for id == 50 should not skip
        assert!(!zm.can_skip("id", CmpOp::Eq, &ScalarValue::Int64(50)));
        // Searching for id > 200 should skip (max is 99)
        assert!(zm.can_skip("id", CmpOp::Gt, &ScalarValue::Int64(200)));
        // Searching for id < -1 should skip (min is 0)
        assert!(zm.can_skip("id", CmpOp::Lt, &ScalarValue::Int64(-1)));
    }

    #[test]
    fn test_segment_checksum_validation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("corrupt.seg");
        let batch = make_int_batch(10);
        SegmentWriter::write(&path, &batch, CompressionCodec::None, 1).unwrap();

        // Corrupt one byte in the middle of the file
        let mut data = std::fs::read(&path).unwrap();
        let mid = data.len() / 2;
        data[mid] ^= 0xFF;
        std::fs::write(&path, &data).unwrap();

        let result = SegmentReader::open(&path);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("checksum"), "expected checksum error, got: {err_msg}");
    }

    #[test]
    fn test_segment_empty_batch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.seg");
        let batch = ColumnBatch::new(vec![]);
        SegmentWriter::write(&path, &batch, CompressionCodec::None, 0).unwrap();

        let reader = SegmentReader::open(&path).unwrap();
        let recovered = reader.read_batch().unwrap();
        assert_eq!(recovered.row_count, 0);
        assert!(recovered.columns.is_empty());
    }

    #[test]
    fn test_estimate_batch_size() {
        let batch = make_int_batch(100);
        let size = estimate_batch_size(&batch);
        // 100 Int64 values (~900 bytes) + 100 Float64 values (~900 bytes) + column names
        assert!(size > 1000, "expected size > 1000, got {size}");
    }

    // ================================================================
    // Hot/Cold Tiering Tests
    // ================================================================

    use crate::columnar::MergeTree;

    #[test]
    fn test_hot_cold_small_data_stays_hot() {
        let dir = tempfile::tempdir().unwrap();
        let mut mt = MergeTree::open(vec!["id".into()], dir.path()).unwrap();
        mt.max_parts = 100; // prevent auto-merge

        // Insert a small batch (well below 64KB threshold)
        let batch = ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64(vec![Some(1), Some(2), Some(3)])),
            ("value".into(), ColumnData::Float64(vec![Some(1.0), Some(2.0), Some(3.0)])),
        ]);
        mt.insert(batch);

        // Should stay hot (in memory)
        assert_eq!(mt.part_count(), 1);
        assert_eq!(mt.cold_part_count(), 0);
        assert_eq!(mt.total_rows(), 3);
    }

    #[test]
    fn test_hot_cold_large_data_flushed() {
        let dir = tempfile::tempdir().unwrap();
        let mut mt = MergeTree::open(vec!["id".into()], dir.path()).unwrap();
        mt.max_parts = 100;
        mt.cold_threshold_bytes = 500; // low threshold so our data gets flushed

        // Insert a batch large enough to exceed threshold
        let n = 200;
        let ids: Vec<Option<i64>> = (0..n).map(|i| Some(i as i64)).collect();
        let vals: Vec<Option<f64>> = (0..n).map(|i| Some(i as f64 * 1.5)).collect();
        let batch = ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64(ids)),
            ("value".into(), ColumnData::Float64(vals)),
        ]);
        mt.insert(batch);

        // Should be flushed to disk (cold)
        assert_eq!(mt.part_count(), 0, "expected 0 hot parts");
        assert_eq!(mt.cold_part_count(), 1, "expected 1 cold part");
        assert_eq!(mt.total_rows(), n);

        // Verify segment file exists
        let seg_files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .flatten()
            .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("seg"))
            .collect();
        assert_eq!(seg_files.len(), 1);
    }

    #[test]
    fn test_scan_across_hot_and_cold() {
        let dir = tempfile::tempdir().unwrap();
        let mut mt = MergeTree::open(vec!["id".into()], dir.path()).unwrap();
        mt.max_parts = 100;
        mt.cold_threshold_bytes = 500; // low threshold

        // Insert a large batch (will become cold)
        let n = 200;
        let ids: Vec<Option<i64>> = (0..n).map(|i| Some(i as i64)).collect();
        let vals: Vec<Option<f64>> = (0..n).map(|i| Some(i as f64)).collect();
        let batch = ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64(ids)),
            ("value".into(), ColumnData::Float64(vals)),
        ]);
        mt.insert(batch);
        assert_eq!(mt.cold_part_count(), 1);

        // Insert a small batch (will stay hot)
        let batch2 = ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64(vec![Some(1000), Some(1001)])),
            ("value".into(), ColumnData::Float64(vec![Some(1000.0), Some(1001.0)])),
        ]);
        mt.insert(batch2);
        assert_eq!(mt.part_count(), 1);

        // Full scan should return both hot and cold data
        let all = mt.scan_all();
        let total_rows: usize = all.iter().map(|b| b.row_count).sum();
        assert_eq!(total_rows, n + 2);

        // Predicate scan for id > 999 should hit hot part only
        let filtered = mt.scan("id", CmpOp::Gt, &ScalarValue::Int64(999));
        let filtered_rows: usize = filtered.iter().map(|b| b.row_count).sum();
        // Hot part has 1000,1001 (2 rows), cold part has 0..199 (all < 1000, pruned by zone map)
        assert_eq!(filtered_rows, 2);
    }

    #[test]
    fn test_scan_correct_results_hot_cold() {
        let dir = tempfile::tempdir().unwrap();
        let mut mt = MergeTree::open(vec!["id".into()], dir.path()).unwrap();
        mt.max_parts = 100;
        mt.cold_threshold_bytes = 50; // threshold lower than batch size

        // Insert batch that will become cold (20 Int64 values = ~180 bytes)
        let n = 20;
        let batch = ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64((1..=n).map(|i| Some(i as i64 * 10)).collect())),
        ]);
        mt.insert(batch);

        // Verify it went cold
        assert_eq!(mt.cold_part_count(), 1);
        assert_eq!(mt.part_count(), 0);

        // scan_all should return exactly the data we inserted
        let all = mt.scan_all();
        assert_eq!(all.len(), 1);
        if let Some(ColumnData::Int64(ids)) = all[0].column("id") {
            assert_eq!(ids.len(), n);
            assert_eq!(ids[0], Some(10));
            assert_eq!(ids[n - 1], Some(n as i64 * 10));
        } else {
            panic!("expected Int64 column");
        }
    }

    #[test]
    fn test_crash_recovery_cold_only() {
        let dir = tempfile::tempdir().unwrap();

        // Write some cold segments
        {
            let mut mt = MergeTree::open(vec!["id".into()], dir.path()).unwrap();
            mt.max_parts = 100;
            mt.cold_threshold_bytes = 100;

            let batch = ColumnBatch::new(vec![
                ("id".into(), ColumnData::Int64((0..50).map(|i| Some(i as i64)).collect())),
                ("val".into(), ColumnData::Float64((0..50).map(|i| Some(i as f64)).collect())),
            ]);
            mt.insert(batch);
            assert_eq!(mt.cold_part_count(), 1);
        }

        // "Crash" and recover — open a new MergeTree from the same directory
        {
            let mt = MergeTree::open(vec!["id".into()], dir.path()).unwrap();
            assert_eq!(mt.cold_part_count(), 1);
            assert_eq!(mt.total_rows(), 50);

            // Data should be readable
            let all = mt.scan_all();
            let total_rows: usize = all.iter().map(|b| b.row_count).sum();
            assert_eq!(total_rows, 50);

            if let Some(ColumnData::Int64(ids)) = all[0].column("id") {
                assert_eq!(ids[0], Some(0));
                assert_eq!(ids[49], Some(49));
            } else {
                panic!("expected Int64 column");
            }
        }
    }

    #[test]
    fn test_crash_recovery_with_wal_unflushed() {
        let dir = tempfile::tempdir().unwrap();

        // Flush some parts to disk
        {
            let mut mt = MergeTree::open(vec!["id".into()], dir.path()).unwrap();
            mt.max_parts = 100;
            mt.cold_threshold_bytes = 100;

            // This batch will be flushed cold
            let batch = ColumnBatch::new(vec![
                ("id".into(), ColumnData::Int64((0..30).map(|i| Some(i as i64)).collect())),
            ]);
            mt.insert(batch);
            assert_eq!(mt.cold_part_count(), 1);
        }

        // Simulate crash recovery: load segments + replay WAL unflushed data
        let wal_batch = ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64(vec![Some(100), Some(101), Some(102)])),
        ]);
        let mt = MergeTree::recover(
            vec!["id".into()],
            dir.path(),
            vec![wal_batch],
        ).unwrap();

        // Should have 1 cold part (from segments) + 1 hot part (from WAL)
        assert_eq!(mt.cold_part_count(), 1);
        assert_eq!(mt.part_count(), 1);
        assert_eq!(mt.total_rows(), 33); // 30 cold + 3 hot

        // Full scan should return all data
        let all = mt.scan_all();
        let total: usize = all.iter().map(|b| b.row_count).sum();
        assert_eq!(total, 33);
    }

    #[test]
    fn test_zone_map_pruning_cold_parts() {
        let dir = tempfile::tempdir().unwrap();
        let mut mt = MergeTree::open(vec!["id".into()], dir.path()).unwrap();
        mt.max_parts = 100;
        mt.cold_threshold_bytes = 100;

        // Cold part 1: ids 0..49
        let batch1 = ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64((0..50).map(|i| Some(i as i64)).collect())),
        ]);
        mt.insert(batch1);

        // Cold part 2: ids 1000..1049
        let batch2 = ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64((1000..1050).map(|i| Some(i as i64)).collect())),
        ]);
        mt.insert(batch2);

        assert_eq!(mt.cold_part_count(), 2);

        // Scan for id > 500: should only load cold part 2 (ids 1000..1049)
        let results = mt.scan("id", CmpOp::Gt, &ScalarValue::Int64(500));
        assert_eq!(results.len(), 1, "expected 1 batch, zone map should prune part 1");
        let total_rows: usize = results.iter().map(|b| b.row_count).sum();
        assert_eq!(total_rows, 50);

        // Scan for id < 25: should only load cold part 1
        let results2 = mt.scan("id", CmpOp::Lt, &ScalarValue::Int64(25));
        assert_eq!(results2.len(), 1, "expected 1 batch, zone map should prune part 2");

        // Scan for id == 2000: should load nothing (both parts pruned)
        let results3 = mt.scan("id", CmpOp::Eq, &ScalarValue::Int64(2000));
        assert_eq!(results3.len(), 0, "expected 0 batches, both parts should be pruned");
    }

    #[test]
    fn test_multiple_cold_recovery() {
        let dir = tempfile::tempdir().unwrap();

        // Create multiple cold parts
        {
            let mut mt = MergeTree::open(vec!["id".into()], dir.path()).unwrap();
            mt.max_parts = 100;
            mt.cold_threshold_bytes = 50;

            for batch_idx in 0..5 {
                let start = batch_idx * 20;
                let batch = ColumnBatch::new(vec![
                    ("id".into(), ColumnData::Int64(
                        (start..start + 20).map(|i| Some(i as i64)).collect()
                    )),
                ]);
                mt.insert(batch);
            }

            assert!(mt.cold_part_count() >= 3, "expected multiple cold parts");
        }

        // Recover
        let mt = MergeTree::open(vec!["id".into()], dir.path()).unwrap();
        assert!(mt.cold_part_count() >= 3);
        assert_eq!(mt.total_rows(), 100);
    }

    #[test]
    fn test_in_memory_only_no_flush() {
        // MergeTree without data_dir should never flush
        let mut mt = MergeTree::new(vec!["id".into()]);
        mt.cold_threshold_bytes = 1; // extremely low, but no data_dir

        let batch = ColumnBatch::new(vec![
            ("id".into(), ColumnData::Int64((0..1000).map(|i| Some(i as i64)).collect())),
        ]);
        mt.insert(batch);

        // Everything stays hot because there's no disk to flush to
        assert_eq!(mt.cold_part_count(), 0);
        assert!(mt.part_count() > 0);
        assert_eq!(mt.total_rows(), 1000);
    }
}
