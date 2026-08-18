//! Row serialization / deserialization to byte tuples.
//!
//! Binary format:
//!   [null_bitmap: ceil(N/8) bytes] [value_1] [value_2] ... [value_N]
//!
//! Each value is encoded as:
//!   Bool:    1 byte (0 or 1)
//!   Int32:   4 bytes LE
//!   Int64:   8 bytes LE
//!   Float64: 8 bytes LE
//!   Text:    4 bytes LE (length) + N bytes UTF-8
//!   Null:    0 bytes (flagged in bitmap)

use crate::types::{DataType, Row, Value};

/// Serialize a row into bytes given the column types.
pub fn serialize_row(row: &Row, col_types: &[DataType]) -> Vec<u8> {
    debug_assert_eq!(row.len(), col_types.len());

    let ncols = row.len();
    let bitmap_bytes = ncols.div_ceil(8);
    let mut buf = vec![0u8; bitmap_bytes];

    // Set null bitmap
    for (i, val) in row.iter().enumerate() {
        if *val == Value::Null {
            buf[i / 8] |= 1 << (i % 8);
        }
    }

    // Encode values — coerce to match declared column type to ensure
    // consistent byte layout (e.g. SQL parser produces Int64 for all
    // integer literals, but INT columns expect 4-byte Int32).
    for (i, val) in row.iter().enumerate() {
        let coerced: std::borrow::Cow<'_, Value> = if i < col_types.len() {
            match (val, &col_types[i]) {
                (Value::Int64(n), DataType::Int32) => {
                    std::borrow::Cow::Owned(Value::Int32(*n as i32))
                }
                (Value::Int32(n), DataType::Int64) => {
                    std::borrow::Cow::Owned(Value::Int64(*n as i64))
                }
                (Value::Int64(n), DataType::Float64) => {
                    std::borrow::Cow::Owned(Value::Float64(*n as f64))
                }
                (Value::Float64(f), DataType::Int32) => {
                    std::borrow::Cow::Owned(Value::Int32(*f as i32))
                }
                (Value::Float64(f), DataType::Int64) => {
                    std::borrow::Cow::Owned(Value::Int64(*f as i64))
                }
                _ => std::borrow::Cow::Borrowed(val),
            }
        } else {
            std::borrow::Cow::Borrowed(val)
        };
        let val = &*coerced;
        match val {
            Value::Null => {} // already flagged in bitmap
            Value::Bool(b) => buf.push(if *b { 1 } else { 0 }),
            Value::Int32(n) => buf.extend_from_slice(&n.to_le_bytes()),
            Value::Int64(n) => buf.extend_from_slice(&n.to_le_bytes()),
            Value::Float64(n) => buf.extend_from_slice(&n.to_le_bytes()),
            Value::Text(s) => {
                let bytes = s.as_bytes();
                buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(bytes);
            }
            Value::Jsonb(v) => {
                // `unwrap_or_default()` here wrote ZERO bytes on failure, and
                // an empty slice is not valid JSON — so a value that failed to
                // serialize came back as null on read, silently. Now that a
                // malformed JSONB payload is reported as corruption, writing
                // unreadable bytes would turn that write into an unreadable
                // row. Store a valid `null` document instead: same observable
                // value as before, but readable. (`to_vec` on a
                // `serde_json::Value` is effectively infallible — Number cannot
                // hold NaN or infinity — so this is a guard, not a path.)
                let bytes = serde_json::to_vec(v).unwrap_or_else(|_| b"null".to_vec());
                buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(&bytes);
            }
            Value::Date(d) => buf.extend_from_slice(&d.to_le_bytes()),
            Value::Timestamp(us) | Value::TimestampTz(us) => {
                buf.extend_from_slice(&us.to_le_bytes())
            }
            Value::Numeric(s) => {
                let bytes = s.as_bytes();
                buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(bytes);
            }
            Value::Uuid(b) => buf.extend_from_slice(b),
            Value::Bytea(b) => {
                buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
                buf.extend_from_slice(b);
            }
            Value::Array(elems) => {
                // Serialize array: [elem_count: u32] [elem_1] [elem_2] ...
                // Each element is: [type_tag: u8] [is_null: u8] [data...]
                // type_tag: 0=Bool, 1=Int32, 2=Int64, 3=Float64, 4=Text, 5=Null
                let mut arr_buf = Vec::new();
                arr_buf.extend_from_slice(&(elems.len() as u32).to_le_bytes());
                for elem in elems {
                    match elem {
                        Value::Null => arr_buf.push(5),
                        Value::Bool(b) => {
                            arr_buf.push(0);
                            arr_buf.push(if *b { 1 } else { 0 });
                        }
                        Value::Int32(n) => {
                            arr_buf.push(1);
                            arr_buf.extend_from_slice(&n.to_le_bytes());
                        }
                        Value::Int64(n) => {
                            arr_buf.push(2);
                            arr_buf.extend_from_slice(&n.to_le_bytes());
                        }
                        Value::Float64(n) => {
                            arr_buf.push(3);
                            arr_buf.extend_from_slice(&n.to_le_bytes());
                        }
                        Value::Text(s) => {
                            arr_buf.push(4);
                            let b = s.as_bytes();
                            arr_buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
                            arr_buf.extend_from_slice(b);
                        }
                        _ => {
                            // Fallback: serialize as text
                            let s = format!("{elem}");
                            arr_buf.push(4);
                            let b = s.as_bytes();
                            arr_buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
                            arr_buf.extend_from_slice(b);
                        }
                    }
                }
                buf.extend_from_slice(&(arr_buf.len() as u32).to_le_bytes());
                buf.extend_from_slice(&arr_buf);
            }
            Value::Vector(vec) => {
                // Serialize vector as packed floats
                buf.extend_from_slice(&(vec.len() as u32).to_le_bytes());
                for f in vec {
                    buf.extend_from_slice(&f.to_le_bytes());
                }
            }
            Value::Interval {
                months,
                days,
                microseconds,
            } => {
                buf.extend_from_slice(&months.to_le_bytes());
                buf.extend_from_slice(&days.to_le_bytes());
                buf.extend_from_slice(&microseconds.to_le_bytes());
            }
        }
        let _ = i; // used above
    }

    buf
}

/// Deserialize a row from bytes given the column types.
pub fn deserialize_row(data: &[u8], col_types: &[DataType]) -> Option<Row> {
    let ncols = col_types.len();
    let bitmap_bytes = ncols.div_ceil(8);
    if data.len() < bitmap_bytes {
        return None;
    }

    let bitmap = &data[..bitmap_bytes];
    let mut pos = bitmap_bytes;
    let mut row = Vec::with_capacity(ncols);

    for (i, dtype) in col_types.iter().enumerate() {
        let is_null = (bitmap[i / 8] >> (i % 8)) & 1 == 1;
        if is_null {
            row.push(Value::Null);
            continue;
        }

        match dtype {
            DataType::Bool => {
                if pos >= data.len() {
                    return None;
                }
                // Only 0 and 1 are encodings of a bool. `!= 0` accepted the
                // 254 byte values the encoder can never produce and answered
                // `true` for every one of them, so a damaged bool byte read
                // back as a plausible value rather than a reported corruption
                // — NU-239's shape with a different type in it.
                row.push(Value::Bool(match data[pos] {
                    0 => false,
                    1 => true,
                    _ => return None,
                }));
                pos += 1;
            }
            DataType::Int32 => {
                if pos + 4 > data.len() {
                    return None;
                }
                let n =
                    i32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
                row.push(Value::Int32(n));
                pos += 4;
            }
            DataType::Int64 => {
                if pos + 8 > data.len() {
                    return None;
                }
                let n = i64::from_le_bytes([
                    data[pos],
                    data[pos + 1],
                    data[pos + 2],
                    data[pos + 3],
                    data[pos + 4],
                    data[pos + 5],
                    data[pos + 6],
                    data[pos + 7],
                ]);
                row.push(Value::Int64(n));
                pos += 8;
            }
            DataType::Float64 => {
                if pos + 8 > data.len() {
                    return None;
                }
                let n = f64::from_le_bytes([
                    data[pos],
                    data[pos + 1],
                    data[pos + 2],
                    data[pos + 3],
                    data[pos + 4],
                    data[pos + 5],
                    data[pos + 6],
                    data[pos + 7],
                ]);
                row.push(Value::Float64(n));
                pos += 8;
            }
            DataType::Text => {
                if pos + 4 > data.len() {
                    return None;
                }
                let len =
                    u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                        as usize;
                pos += 4;
                if pos + len > data.len() {
                    return None;
                }
                let s = std::str::from_utf8(&data[pos..pos + len]).ok()?;
                row.push(Value::Text(s.to_string()));
                pos += len;
            }
            DataType::Jsonb => {
                if pos + 4 > data.len() {
                    return None;
                }
                let len =
                    u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                        as usize;
                pos += 4;
                if pos + len > data.len() {
                    return None;
                }
                // JSONB used to be the ONE type that routed around the
                // corruption gate: `.unwrap_or(Value::Null)`, justified by
                // "malformed stored JSONB must not drop the row". That was true
                // when a `None` here meant the row was silently dropped from
                // the scan. It is not true any more — `deserialize_row`
                // returning None now raises `StorageError::Corruption` naming
                // the page, slot and table. So the choice is no longer "lose
                // the row or null the column", it is "report corruption or
                // answer a query with a value that was never stored", and a
                // silent wrong answer is the worse of those.
                let Ok(v) = serde_json::from_slice::<serde_json::Value>(&data[pos..pos + len])
                else {
                    return None;
                };
                row.push(Value::Jsonb(v));
                pos += len;
            }
            DataType::Date => {
                if pos + 4 > data.len() {
                    return None;
                }
                let d =
                    i32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
                row.push(Value::Date(d));
                pos += 4;
            }
            DataType::Timestamp => {
                if pos + 8 > data.len() {
                    return None;
                }
                let us = i64::from_le_bytes([
                    data[pos],
                    data[pos + 1],
                    data[pos + 2],
                    data[pos + 3],
                    data[pos + 4],
                    data[pos + 5],
                    data[pos + 6],
                    data[pos + 7],
                ]);
                row.push(Value::Timestamp(us));
                pos += 8;
            }
            DataType::TimestampTz => {
                if pos + 8 > data.len() {
                    return None;
                }
                let us = i64::from_le_bytes([
                    data[pos],
                    data[pos + 1],
                    data[pos + 2],
                    data[pos + 3],
                    data[pos + 4],
                    data[pos + 5],
                    data[pos + 6],
                    data[pos + 7],
                ]);
                row.push(Value::TimestampTz(us));
                pos += 8;
            }
            DataType::Numeric => {
                if pos + 4 > data.len() {
                    return None;
                }
                let len =
                    u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                        as usize;
                pos += 4;
                if pos + len > data.len() {
                    return None;
                }
                let s = std::str::from_utf8(&data[pos..pos + len]).ok()?;
                row.push(Value::Numeric(s.to_string()));
                pos += len;
            }
            DataType::Uuid => {
                if pos + 16 > data.len() {
                    return None;
                }
                let mut b = [0u8; 16];
                b.copy_from_slice(&data[pos..pos + 16]);
                row.push(Value::Uuid(b));
                pos += 16;
            }
            DataType::Bytea => {
                if pos + 4 > data.len() {
                    return None;
                }
                let len =
                    u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                        as usize;
                pos += 4;
                if pos + len > data.len() {
                    return None;
                }
                row.push(Value::Bytea(data[pos..pos + len].to_vec()));
                pos += len;
            }
            DataType::Array(_) => {
                // Deserialize array: [total_len: u32] [elem_count: u32] [elements...]
                if pos + 4 > data.len() {
                    return None;
                }
                let total_len =
                    u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                        as usize;
                pos += 4;
                if pos + total_len > data.len() {
                    return None;
                }
                let arr_end = pos + total_len;
                // Read elem_count
                if pos + 4 > arr_end {
                    return None;
                }
                let elem_count =
                    u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                        as usize;
                pos += 4;
                let mut elems = Vec::with_capacity(super::wal_util::bounded_capacity(elem_count));
                for _ in 0..elem_count {
                    if pos >= arr_end {
                        return None;
                    }
                    let tag = data[pos];
                    pos += 1;
                    match tag {
                        0 => {
                            // Bool
                            if pos >= arr_end {
                                return None;
                            }
                            elems.push(Value::Bool(match data[pos] {
                                0 => false,
                                1 => true,
                                _ => return None,
                            }));
                            pos += 1;
                        }
                        1 => {
                            // Int32
                            if pos + 4 > arr_end {
                                return None;
                            }
                            let n = i32::from_le_bytes([
                                data[pos],
                                data[pos + 1],
                                data[pos + 2],
                                data[pos + 3],
                            ]);
                            elems.push(Value::Int32(n));
                            pos += 4;
                        }
                        2 => {
                            // Int64
                            if pos + 8 > arr_end {
                                return None;
                            }
                            let n = i64::from_le_bytes([
                                data[pos],
                                data[pos + 1],
                                data[pos + 2],
                                data[pos + 3],
                                data[pos + 4],
                                data[pos + 5],
                                data[pos + 6],
                                data[pos + 7],
                            ]);
                            elems.push(Value::Int64(n));
                            pos += 8;
                        }
                        3 => {
                            // Float64
                            if pos + 8 > arr_end {
                                return None;
                            }
                            let n = f64::from_le_bytes([
                                data[pos],
                                data[pos + 1],
                                data[pos + 2],
                                data[pos + 3],
                                data[pos + 4],
                                data[pos + 5],
                                data[pos + 6],
                                data[pos + 7],
                            ]);
                            elems.push(Value::Float64(n));
                            pos += 8;
                        }
                        4 => {
                            // Text
                            if pos + 4 > arr_end {
                                return None;
                            }
                            let slen = u32::from_le_bytes([
                                data[pos],
                                data[pos + 1],
                                data[pos + 2],
                                data[pos + 3],
                            ]) as usize;
                            pos += 4;
                            if pos + slen > arr_end {
                                return None;
                            }
                            let s = std::str::from_utf8(&data[pos..pos + slen]).ok()?;
                            elems.push(Value::Text(s.to_string()));
                            pos += slen;
                        }
                        5 => {
                            // Null
                            elems.push(Value::Null);
                        }
                        _ => return None, // Unknown tag
                    }
                }
                // The array carries BOTH a byte span and an element count,
                // and they are the only redundancy in the encoding. Trusting
                // the count alone and jumping to `arr_end` meant a damaged
                // count decoded 36 bytes of live elements as an EMPTY array
                // and reported success. They must agree.
                if pos != arr_end {
                    return None;
                }
                row.push(Value::Array(elems));
            }
            DataType::Vector(_) => {
                // Deserialize packed floats.
                //
                // The stored tuple is self-describing: it carries its own
                // element count, so the DECLARED dimension must not gate the
                // decode. It used to (`count != *dim` returned None), and a
                // column declared as plain `VECTOR` — which `convert_data_type`
                // maps to `Vector(0)`, meaning "dimension unknown" — therefore
                // rejected every row it had itself written. The scan logged
                // "failed to deserialize tuple" and SKIPPED the row, so a table
                // with a dimensionless VECTOR column returned zero rows from
                // any query that touched the heap while `SELECT id` still
                // answered from the index. Silent, total, unattributed data
                // loss; see `deserialize_size` and `deserialize_column_at` for
                // the same check.
                if pos + 4 > data.len() {
                    return None;
                }
                let count =
                    u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                        as usize;
                pos += 4;
                if pos + count * 4 > data.len() {
                    return None;
                }
                let mut vec = Vec::with_capacity(count);
                for _ in 0..count {
                    let f = f32::from_le_bytes([
                        data[pos],
                        data[pos + 1],
                        data[pos + 2],
                        data[pos + 3],
                    ]);
                    vec.push(f);
                    pos += 4;
                }
                row.push(Value::Vector(vec));
            }
            DataType::Interval => {
                if pos + 16 > data.len() {
                    return None;
                }
                let months =
                    i32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
                pos += 4;
                let days =
                    i32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
                pos += 4;
                let microseconds = i64::from_le_bytes([
                    data[pos],
                    data[pos + 1],
                    data[pos + 2],
                    data[pos + 3],
                    data[pos + 4],
                    data[pos + 5],
                    data[pos + 6],
                    data[pos + 7],
                ]);
                pos += 8;
                row.push(Value::Interval {
                    months,
                    days,
                    microseconds,
                });
            }
            DataType::UserDefined(_) => {
                // Enum values stored as length-prefixed UTF-8, same as Text.
                if pos + 4 > data.len() {
                    return None;
                }
                let len =
                    u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                        as usize;
                pos += 4;
                if pos + len > data.len() {
                    return None;
                }
                let s = std::str::from_utf8(&data[pos..pos + len]).ok()?.to_string();
                pos += len;
                row.push(Value::Text(s));
            }
        }
    }

    Some(row)
}

/// Compute the byte size of a single non-null column value at `data[pos..]`.
///
/// Returns `None` if the data is too short. This is used by
/// `deserialize_row_projected` to skip over columns that are not in the
/// projection set without decoding them.
fn column_byte_size(data: &[u8], pos: usize, dtype: &DataType) -> Option<usize> {
    match dtype {
        DataType::Bool => {
            if pos >= data.len() {
                return None;
            }
            Some(1)
        }
        DataType::Int32 | DataType::Date => {
            if pos + 4 > data.len() {
                return None;
            }
            Some(4)
        }
        DataType::Int64 | DataType::Float64 | DataType::Timestamp | DataType::TimestampTz => {
            if pos + 8 > data.len() {
                return None;
            }
            Some(8)
        }
        DataType::Text | DataType::Numeric | DataType::Bytea | DataType::UserDefined(_) => {
            if pos + 4 > data.len() {
                return None;
            }
            let len = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                as usize;
            if pos + 4 + len > data.len() {
                return None;
            }
            Some(4 + len)
        }
        DataType::Jsonb => {
            if pos + 4 > data.len() {
                return None;
            }
            let len = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                as usize;
            if pos + 4 + len > data.len() {
                return None;
            }
            Some(4 + len)
        }
        DataType::Uuid => {
            if pos + 16 > data.len() {
                return None;
            }
            Some(16)
        }
        DataType::Array(_) => {
            // Format: [total_len: u32] [array_data: total_len bytes]
            if pos + 4 > data.len() {
                return None;
            }
            let total_len =
                u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                    as usize;
            if pos + 4 + total_len > data.len() {
                return None;
            }
            Some(4 + total_len)
        }
        DataType::Vector(_) => {
            // Format: [count: u32] [count * f32]. The count is authoritative;
            // the declared dimension is not a decode gate (see `deserialize_row`).
            if pos + 4 > data.len() {
                return None;
            }
            let count = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                as usize;
            if pos + 4 + count * 4 > data.len() {
                return None;
            }
            Some(4 + count * 4)
        }
        DataType::Interval => {
            // months(i32) + days(i32) + microseconds(i64) = 16 bytes
            if pos + 16 > data.len() {
                return None;
            }
            Some(16)
        }
    }
}

/// Deserialize only the projected columns from a serialized row.
///
/// `projection` contains the column indices (into `col_types`) to decode.
/// The returned `Row` contains only those columns, in the order they appear
/// in the `projection` slice. Non-projected columns are skipped efficiently
/// by advancing the cursor past their bytes without allocating.
///
/// This avoids decoding and allocating values for columns the query does not
/// need, which can save 50-70% of deserialization time for wide tables when
/// only a few columns are selected.
pub fn deserialize_row_projected(
    data: &[u8],
    col_types: &[DataType],
    projection: &[usize],
) -> Option<Row> {
    let ncols = col_types.len();
    let bitmap_bytes = ncols.div_ceil(8);
    if data.len() < bitmap_bytes {
        return None;
    }

    // Fast path: if projection covers all columns, use the standard path
    if projection.len() == ncols {
        let mut is_identity = true;
        for (i, &p) in projection.iter().enumerate() {
            if p != i {
                is_identity = false;
                break;
            }
        }
        if is_identity {
            return deserialize_row(data, col_types);
        }
    }

    let bitmap = &data[..bitmap_bytes];
    let mut pos = bitmap_bytes;

    // First pass: compute positions of each column. Columns are variable-length,
    // so a projected column's offset is only known by walking the ones before it
    // — but nothing after the last projected column ever needs an offset, so the
    // walk stops there. Projecting an early column out of a wide row skips the
    // size computation for the whole tail.
    let Some(&last_needed) = projection.iter().max() else {
        return Some(Vec::new()); // zero-column projection: the row still exists
    };
    if last_needed >= ncols {
        return None;
    }
    let mut col_positions: Vec<(usize, bool)> = Vec::with_capacity(last_needed + 1);
    for (i, dtype) in col_types.iter().enumerate().take(last_needed + 1) {
        let is_null = (bitmap[i / 8] >> (i % 8)) & 1 == 1;
        col_positions.push((pos, is_null));
        if !is_null {
            let size = column_byte_size(data, pos, dtype)?;
            pos += size;
        }
    }

    // Second pass: decode only projected columns in projection order.
    let mut row = Vec::with_capacity(projection.len());
    for &col_idx in projection {
        if col_idx >= ncols {
            return None;
        }
        let (col_pos, is_null) = col_positions[col_idx];
        if is_null {
            row.push(Value::Null);
            continue;
        }

        let dtype = &col_types[col_idx];
        let val = decode_column_at(data, col_pos, dtype)?;
        row.push(val);
    }

    Some(row)
}

/// Decode a single column value at `data[pos..]` given its type.
fn decode_column_at(data: &[u8], pos: usize, dtype: &DataType) -> Option<Value> {
    match dtype {
        DataType::Bool => {
            if pos >= data.len() {
                return None;
            }
            match data[pos] {
                0 => Some(Value::Bool(false)),
                1 => Some(Value::Bool(true)),
                _ => None,
            }
        }
        DataType::Int32 => {
            if pos + 4 > data.len() {
                return None;
            }
            let n = i32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
            Some(Value::Int32(n))
        }
        DataType::Int64 => {
            if pos + 8 > data.len() {
                return None;
            }
            let n = i64::from_le_bytes([
                data[pos],
                data[pos + 1],
                data[pos + 2],
                data[pos + 3],
                data[pos + 4],
                data[pos + 5],
                data[pos + 6],
                data[pos + 7],
            ]);
            Some(Value::Int64(n))
        }
        DataType::Float64 => {
            if pos + 8 > data.len() {
                return None;
            }
            let n = f64::from_le_bytes([
                data[pos],
                data[pos + 1],
                data[pos + 2],
                data[pos + 3],
                data[pos + 4],
                data[pos + 5],
                data[pos + 6],
                data[pos + 7],
            ]);
            Some(Value::Float64(n))
        }
        DataType::Text => {
            if pos + 4 > data.len() {
                return None;
            }
            let len = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                as usize;
            let start = pos + 4;
            if start + len > data.len() {
                return None;
            }
            let s = std::str::from_utf8(&data[start..start + len]).ok()?;
            Some(Value::Text(s.to_string()))
        }
        DataType::Jsonb => {
            if pos + 4 > data.len() {
                return None;
            }
            let len = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                as usize;
            let start = pos + 4;
            if start + len > data.len() {
                return None;
            }
            // Same as the full-row path above: report, do not invent a value.
            // A projected read that answered `null` where the full read
            // errored would also make the two paths disagree about the same
            // bytes.
            let v: serde_json::Value = serde_json::from_slice(&data[start..start + len]).ok()?;
            Some(Value::Jsonb(v))
        }
        DataType::Date => {
            if pos + 4 > data.len() {
                return None;
            }
            let d = i32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
            Some(Value::Date(d))
        }
        DataType::Timestamp => {
            if pos + 8 > data.len() {
                return None;
            }
            let us = i64::from_le_bytes([
                data[pos],
                data[pos + 1],
                data[pos + 2],
                data[pos + 3],
                data[pos + 4],
                data[pos + 5],
                data[pos + 6],
                data[pos + 7],
            ]);
            Some(Value::Timestamp(us))
        }
        DataType::TimestampTz => {
            if pos + 8 > data.len() {
                return None;
            }
            let us = i64::from_le_bytes([
                data[pos],
                data[pos + 1],
                data[pos + 2],
                data[pos + 3],
                data[pos + 4],
                data[pos + 5],
                data[pos + 6],
                data[pos + 7],
            ]);
            Some(Value::TimestampTz(us))
        }
        DataType::Numeric => {
            if pos + 4 > data.len() {
                return None;
            }
            let len = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                as usize;
            let start = pos + 4;
            if start + len > data.len() {
                return None;
            }
            let s = std::str::from_utf8(&data[start..start + len]).ok()?;
            Some(Value::Numeric(s.to_string()))
        }
        DataType::Uuid => {
            if pos + 16 > data.len() {
                return None;
            }
            let mut b = [0u8; 16];
            b.copy_from_slice(&data[pos..pos + 16]);
            Some(Value::Uuid(b))
        }
        DataType::Bytea => {
            if pos + 4 > data.len() {
                return None;
            }
            let len = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                as usize;
            let start = pos + 4;
            if start + len > data.len() {
                return None;
            }
            Some(Value::Bytea(data[start..start + len].to_vec()))
        }
        DataType::Array(_) => {
            if pos + 4 > data.len() {
                return None;
            }
            let total_len =
                u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                    as usize;
            let arr_start = pos + 4;
            if arr_start + total_len > data.len() {
                return None;
            }
            let arr_end = arr_start + total_len;
            let mut apos = arr_start;
            if apos + 4 > arr_end {
                return None;
            }
            let elem_count =
                u32::from_le_bytes([data[apos], data[apos + 1], data[apos + 2], data[apos + 3]])
                    as usize;
            apos += 4;
            let mut elems = Vec::with_capacity(elem_count);
            for _ in 0..elem_count {
                if apos >= arr_end {
                    return None;
                }
                let tag = data[apos];
                apos += 1;
                match tag {
                    0 => {
                        if apos >= arr_end {
                            return None;
                        }
                        elems.push(Value::Bool(match data[apos] {
                            0 => false,
                            1 => true,
                            _ => return None,
                        }));
                        apos += 1;
                    }
                    1 => {
                        if apos + 4 > arr_end {
                            return None;
                        }
                        let n = i32::from_le_bytes([
                            data[apos],
                            data[apos + 1],
                            data[apos + 2],
                            data[apos + 3],
                        ]);
                        elems.push(Value::Int32(n));
                        apos += 4;
                    }
                    2 => {
                        if apos + 8 > arr_end {
                            return None;
                        }
                        let n = i64::from_le_bytes([
                            data[apos],
                            data[apos + 1],
                            data[apos + 2],
                            data[apos + 3],
                            data[apos + 4],
                            data[apos + 5],
                            data[apos + 6],
                            data[apos + 7],
                        ]);
                        elems.push(Value::Int64(n));
                        apos += 8;
                    }
                    3 => {
                        if apos + 8 > arr_end {
                            return None;
                        }
                        let n = f64::from_le_bytes([
                            data[apos],
                            data[apos + 1],
                            data[apos + 2],
                            data[apos + 3],
                            data[apos + 4],
                            data[apos + 5],
                            data[apos + 6],
                            data[apos + 7],
                        ]);
                        elems.push(Value::Float64(n));
                        apos += 8;
                    }
                    4 => {
                        if apos + 4 > arr_end {
                            return None;
                        }
                        let slen = u32::from_le_bytes([
                            data[apos],
                            data[apos + 1],
                            data[apos + 2],
                            data[apos + 3],
                        ]) as usize;
                        apos += 4;
                        if apos + slen > arr_end {
                            return None;
                        }
                        let s = std::str::from_utf8(&data[apos..apos + slen]).ok()?;
                        elems.push(Value::Text(s.to_string()));
                        apos += slen;
                    }
                    5 => {
                        elems.push(Value::Null);
                    }
                    _ => return None,
                }
            }
            // Same span/count agreement as the full-row path above.
            if apos != arr_end {
                return None;
            }
            Some(Value::Array(elems))
        }
        DataType::Vector(_) => {
            // The stored count is authoritative; the declared dimension is not
            // a decode gate (see `deserialize_row`).
            if pos + 4 > data.len() {
                return None;
            }
            let count = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                as usize;
            let start = pos + 4;
            if start + count * 4 > data.len() {
                return None;
            }
            let mut vec = Vec::with_capacity(count);
            let mut fpos = start;
            for _ in 0..count {
                let f = f32::from_le_bytes([
                    data[fpos],
                    data[fpos + 1],
                    data[fpos + 2],
                    data[fpos + 3],
                ]);
                vec.push(f);
                fpos += 4;
            }
            Some(Value::Vector(vec))
        }
        DataType::Interval => {
            if pos + 16 > data.len() {
                return None;
            }
            let months =
                i32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
            let days =
                i32::from_le_bytes([data[pos + 4], data[pos + 5], data[pos + 6], data[pos + 7]]);
            let microseconds = i64::from_le_bytes([
                data[pos + 8],
                data[pos + 9],
                data[pos + 10],
                data[pos + 11],
                data[pos + 12],
                data[pos + 13],
                data[pos + 14],
                data[pos + 15],
            ]);
            Some(Value::Interval {
                months,
                days,
                microseconds,
            })
        }
        DataType::UserDefined(_) => {
            if pos + 4 > data.len() {
                return None;
            }
            let len = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                as usize;
            let start = pos + 4;
            if start + len > data.len() {
                return None;
            }
            let s = std::str::from_utf8(&data[start..start + len])
                .ok()?
                .to_string();
            Some(Value::Text(s))
        }
    }
}

#[cfg(test)]
mod tests {
    // 3.14/3.14159 here are arbitrary test fixtures, not PI approximations.
    #![allow(clippy::approx_constant)]
    use super::*;

    /// NU-239: stored JSONB bytes that do not parse used to decode as JSON
    /// `null`, so a corrupt document read back as a legitimate SQL value.
    ///
    /// JSONB was the one type routing around the corruption gate. The comment
    /// justifying it — "malformed stored JSONB must not drop the row" — was
    /// true when a `None` from here meant the row was silently dropped from a
    /// scan. Since that became `StorageError::Corruption` naming page, slot and
    /// table, the trade is no longer "lose the row or null the column"; it is
    /// "report corruption or answer with a value nobody stored".
    #[test]
    fn corrupt_jsonb_is_reported_not_read_as_null() {
        let types = vec![DataType::Int32, DataType::Jsonb];
        let row = vec![Value::Int32(1), Value::Jsonb(serde_json::json!({"k": "v"}))];
        let mut bytes = serialize_row(&row, &types);

        // Intact bytes round-trip, so the fixture is real.
        let clean = deserialize_row(&bytes, &types).expect("valid JSONB must decode");
        assert_eq!(clean[1], Value::Jsonb(serde_json::json!({"k": "v"})));

        // Corrupt the JSON payload only; the length prefix stays honest, so
        // this is a bad document rather than a truncated record.
        let n = bytes.len();
        bytes[n - 3] = b'~';
        bytes[n - 2] = b'~';

        assert!(
            deserialize_row(&bytes, &types).is_none(),
            "corrupt JSONB must reach the corruption gate, not decode as null"
        );

        // The projected path must agree with the full path about the same
        // bytes — otherwise which answer you get depends on the query plan.
        assert!(
            deserialize_row_projected(&bytes, &types, &[1]).is_none(),
            "the projected read must report corruption too"
        );
    }

    /// A JSONB value that legitimately IS null still round-trips. A fix that
    /// treated the null document as corruption would pass the test above.
    #[test]
    fn a_genuine_json_null_still_round_trips() {
        let types = vec![DataType::Jsonb];
        let row = vec![Value::Jsonb(serde_json::Value::Null)];
        let bytes = serialize_row(&row, &types);
        let out = deserialize_row(&bytes, &types).expect("a null JSON document is valid");
        assert_eq!(out[0], Value::Jsonb(serde_json::Value::Null));
    }

    #[test]
    fn roundtrip_basic() {
        let types = vec![DataType::Int32, DataType::Text, DataType::Bool];
        let row = vec![
            Value::Int32(42),
            Value::Text("hello".into()),
            Value::Bool(true),
        ];
        let bytes = serialize_row(&row, &types);
        let decoded = deserialize_row(&bytes, &types).unwrap();
        assert_eq!(row, decoded);
    }

    #[test]
    fn roundtrip_nulls() {
        let types = vec![DataType::Int32, DataType::Text, DataType::Float64];
        let row = vec![Value::Null, Value::Text("ok".into()), Value::Null];
        let bytes = serialize_row(&row, &types);
        let decoded = deserialize_row(&bytes, &types).unwrap();
        assert_eq!(row, decoded);
    }

    #[test]
    fn roundtrip_all_types() {
        let types = vec![
            DataType::Bool,
            DataType::Int32,
            DataType::Int64,
            DataType::Float64,
            DataType::Text,
        ];
        let row = vec![
            Value::Bool(false),
            Value::Int32(-100),
            Value::Int64(9999999999),
            Value::Float64(3.14),
            Value::Text("Nucleus".into()),
        ];
        let bytes = serialize_row(&row, &types);
        let decoded = deserialize_row(&bytes, &types).unwrap();
        assert_eq!(row, decoded);
    }

    #[test]
    fn roundtrip_array_int() {
        let types = vec![DataType::Array(Box::new(DataType::Int32))];
        let row = vec![Value::Array(vec![
            Value::Int32(1),
            Value::Int32(2),
            Value::Int32(3),
        ])];
        let bytes = serialize_row(&row, &types);
        let decoded = deserialize_row(&bytes, &types).unwrap();
        assert_eq!(row, decoded);
    }

    #[test]
    fn roundtrip_array_text() {
        let types = vec![DataType::Array(Box::new(DataType::Text))];
        let row = vec![Value::Array(vec![
            Value::Text("hello".into()),
            Value::Text("world".into()),
        ])];
        let bytes = serialize_row(&row, &types);
        let decoded = deserialize_row(&bytes, &types).unwrap();
        assert_eq!(row, decoded);
    }

    #[test]
    fn roundtrip_array_mixed() {
        let types = vec![DataType::Array(Box::new(DataType::Int32))];
        let row = vec![Value::Array(vec![
            Value::Int32(42),
            Value::Null,
            Value::Int32(-1),
        ])];
        let bytes = serialize_row(&row, &types);
        let decoded = deserialize_row(&bytes, &types).unwrap();
        assert_eq!(row, decoded);
    }

    #[test]
    fn roundtrip_array_empty() {
        let types = vec![DataType::Array(Box::new(DataType::Int32))];
        let row = vec![Value::Array(vec![])];
        let bytes = serialize_row(&row, &types);
        let decoded = deserialize_row(&bytes, &types).unwrap();
        assert_eq!(row, decoded);
    }

    /// A `VECTOR` column declared without a dimension is `Vector(0)`, meaning
    /// "dimension unknown" — but the decoder read that 0 as a REQUIREMENT and
    /// rejected every row it had itself written. The scan then logged a
    /// deserialize failure and skipped the row, so a table with a
    /// dimensionless vector column answered `SELECT id` from the index with
    /// every row and `SELECT *` from the heap with none.
    ///
    /// The tuple is self-describing: its own element count decides.
    #[test]
    fn roundtrip_vector_with_undeclared_dimension() {
        let types = vec![DataType::Vector(0)];
        let row = vec![Value::Vector(vec![1.0, 0.0, 0.0])];
        let bytes = serialize_row(&row, &types);

        assert_eq!(
            deserialize_row(&bytes, &types).as_ref(),
            Some(&row),
            "a dimensionless VECTOR column must read back what it wrote"
        );
        assert_eq!(
            deserialize_row_projected(&bytes, &types, &[0]),
            Some(row),
            "projected decode must agree with the whole-row decode"
        );

        // A projection that SKIPS the vector goes through the size-only path,
        // which carried the same check and so also failed the whole row.
        let types = vec![DataType::Vector(0), DataType::Text];
        let row = vec![Value::Vector(vec![1.0, 0.0, 0.0]), Value::Text("v1".into())];
        let bytes = serialize_row(&row, &types);
        assert_eq!(
            deserialize_row_projected(&bytes, &types, &[1]),
            Some(vec![Value::Text("v1".into())]),
            "skipping a dimensionless vector must not fail the row"
        );
        assert_eq!(deserialize_row(&bytes, &types), Some(row));
    }

    /// A vector whose length differs from the column's DECLARED dimension is
    /// accepted at INSERT time today. Whatever the right answer is there, the
    /// decoder must not be the place it is discovered: rejecting the row here
    /// makes already-written data unreadable instead of reporting an error.
    #[test]
    fn stored_vector_decodes_even_when_it_differs_from_the_declared_dimension() {
        let types = vec![DataType::Vector(4)];
        let row = vec![Value::Vector(vec![1.0, 0.0, 0.0])];
        let bytes = serialize_row(&row, &types);
        assert_eq!(deserialize_row(&bytes, &types).as_ref(), Some(&row));
    }

    #[test]
    fn roundtrip_vector() {
        let types = vec![DataType::Vector(3)];
        let row = vec![Value::Vector(vec![1.0, 2.5, -3.0])];
        let bytes = serialize_row(&row, &types);
        let decoded = deserialize_row(&bytes, &types).unwrap();
        assert_eq!(row, decoded);
    }

    #[test]
    fn roundtrip_all_extended_types() {
        let types = vec![
            DataType::Date,
            DataType::Timestamp,
            DataType::TimestampTz,
            DataType::Numeric,
            DataType::Uuid,
            DataType::Bytea,
            DataType::Jsonb,
        ];
        let row = vec![
            Value::Date(18000), // some date
            Value::Timestamp(1700000000000000),
            Value::TimestampTz(1700000000000000),
            Value::Numeric("3.14159".into()),
            Value::Uuid([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]),
            Value::Bytea(vec![0xDE, 0xAD, 0xBE, 0xEF]),
            Value::Jsonb(serde_json::json!({"key": "value", "num": 42})),
        ];
        let bytes = serialize_row(&row, &types);
        let decoded = deserialize_row(&bytes, &types).unwrap();
        assert_eq!(row, decoded);
    }

    #[test]
    fn roundtrip_row_with_array_column() {
        let types = vec![
            DataType::Int32,
            DataType::Array(Box::new(DataType::Text)),
            DataType::Bool,
        ];
        let row = vec![
            Value::Int32(1),
            Value::Array(vec![Value::Text("a".into()), Value::Text("b".into())]),
            Value::Bool(true),
        ];
        let bytes = serialize_row(&row, &types);
        let decoded = deserialize_row(&bytes, &types).unwrap();
        assert_eq!(row, decoded);
    }

    // ================================================================
    // Projected deserialization tests
    // ================================================================

    #[test]
    fn projected_subset_columns() {
        let types = vec![
            DataType::Int32,
            DataType::Text,
            DataType::Bool,
            DataType::Float64,
        ];
        let row = vec![
            Value::Int32(42),
            Value::Text("hello".into()),
            Value::Bool(true),
            Value::Float64(3.14),
        ];
        let bytes = serialize_row(&row, &types);

        // Project only columns 0 and 3 (Int32, Float64)
        let projected = deserialize_row_projected(&bytes, &types, &[0, 3]).unwrap();
        assert_eq!(projected, vec![Value::Int32(42), Value::Float64(3.14)]);
    }

    #[test]
    fn projected_single_column() {
        let types = vec![DataType::Int32, DataType::Text, DataType::Bool];
        let row = vec![
            Value::Int32(99),
            Value::Text("skip me".into()),
            Value::Bool(false),
        ];
        let bytes = serialize_row(&row, &types);

        let projected = deserialize_row_projected(&bytes, &types, &[1]).unwrap();
        assert_eq!(projected, vec![Value::Text("skip me".into())]);
    }

    #[test]
    fn projected_with_nulls() {
        let types = vec![DataType::Int32, DataType::Text, DataType::Float64];
        let row = vec![Value::Null, Value::Text("ok".into()), Value::Null];
        let bytes = serialize_row(&row, &types);

        let projected = deserialize_row_projected(&bytes, &types, &[0, 2]).unwrap();
        assert_eq!(projected, vec![Value::Null, Value::Null]);
    }

    #[test]
    fn projected_identity_equals_full() {
        let types = vec![DataType::Int32, DataType::Text, DataType::Bool];
        let row = vec![
            Value::Int32(42),
            Value::Text("hello".into()),
            Value::Bool(true),
        ];
        let bytes = serialize_row(&row, &types);

        let full = deserialize_row(&bytes, &types).unwrap();
        let projected = deserialize_row_projected(&bytes, &types, &[0, 1, 2]).unwrap();
        assert_eq!(full, projected);
    }

    #[test]
    fn projected_reordered_columns() {
        let types = vec![DataType::Int32, DataType::Text, DataType::Bool];
        let row = vec![
            Value::Int32(42),
            Value::Text("hello".into()),
            Value::Bool(true),
        ];
        let bytes = serialize_row(&row, &types);

        // Project in reverse order
        let projected = deserialize_row_projected(&bytes, &types, &[2, 0]).unwrap();
        assert_eq!(projected, vec![Value::Bool(true), Value::Int32(42)]);
    }

    #[test]
    fn projected_with_variable_length_types() {
        let types = vec![
            DataType::Text,
            DataType::Bytea,
            DataType::Int32,
            DataType::Array(Box::new(DataType::Int32)),
            DataType::Bool,
        ];
        let row = vec![
            Value::Text("first".into()),
            Value::Bytea(vec![0xDE, 0xAD]),
            Value::Int32(7),
            Value::Array(vec![Value::Int32(1), Value::Int32(2)]),
            Value::Bool(true),
        ];
        let bytes = serialize_row(&row, &types);

        // Skip the variable-length Text and Bytea, get Int32 and Bool
        let projected = deserialize_row_projected(&bytes, &types, &[2, 4]).unwrap();
        assert_eq!(projected, vec![Value::Int32(7), Value::Bool(true)]);
    }

    #[test]
    fn projected_all_extended_types() {
        let types = vec![
            DataType::Date,
            DataType::Timestamp,
            DataType::TimestampTz,
            DataType::Numeric,
            DataType::Uuid,
            DataType::Bytea,
            DataType::Jsonb,
        ];
        let row = vec![
            Value::Date(18000),
            Value::Timestamp(1700000000000000),
            Value::TimestampTz(1700000000000000),
            Value::Numeric("3.14159".into()),
            Value::Uuid([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]),
            Value::Bytea(vec![0xDE, 0xAD, 0xBE, 0xEF]),
            Value::Jsonb(serde_json::json!({"key": "value"})),
        ];
        let bytes = serialize_row(&row, &types);

        // Project a mix: Uuid (col 4), Date (col 0), Jsonb (col 6)
        let projected = deserialize_row_projected(&bytes, &types, &[4, 0, 6]).unwrap();
        assert_eq!(projected.len(), 3);
        assert_eq!(
            projected[0],
            Value::Uuid([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16])
        );
        assert_eq!(projected[1], Value::Date(18000));
        assert_eq!(
            projected[2],
            Value::Jsonb(serde_json::json!({"key": "value"}))
        );
    }

    #[test]
    fn projected_empty_projection() {
        let types = vec![DataType::Int32, DataType::Text];
        let row = vec![Value::Int32(1), Value::Text("hi".into())];
        let bytes = serialize_row(&row, &types);

        let projected = deserialize_row_projected(&bytes, &types, &[]).unwrap();
        assert!(projected.is_empty());
    }

    #[test]
    fn projected_vector_and_interval() {
        let types = vec![
            DataType::Int32,
            DataType::Vector(3),
            DataType::Interval,
            DataType::Bool,
        ];
        let row = vec![
            Value::Int32(1),
            Value::Vector(vec![1.0, 2.0, 3.0]),
            Value::Interval {
                months: 1,
                days: 2,
                microseconds: 3000,
            },
            Value::Bool(false),
        ];
        let bytes = serialize_row(&row, &types);

        // Skip vector and interval, get Int32 and Bool
        let projected = deserialize_row_projected(&bytes, &types, &[0, 3]).unwrap();
        assert_eq!(projected, vec![Value::Int32(1), Value::Bool(false)]);

        // Get only the vector
        let projected = deserialize_row_projected(&bytes, &types, &[1]).unwrap();
        assert_eq!(projected, vec![Value::Vector(vec![1.0, 2.0, 3.0])]);
    }
}
