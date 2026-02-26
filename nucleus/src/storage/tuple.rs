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

    // Encode values
    for (i, val) in row.iter().enumerate() {
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
                let bytes = serde_json::to_vec(v).unwrap_or_default();
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
                        Value::Bool(b) => { arr_buf.push(0); arr_buf.push(if *b { 1 } else { 0 }); }
                        Value::Int32(n) => { arr_buf.push(1); arr_buf.extend_from_slice(&n.to_le_bytes()); }
                        Value::Int64(n) => { arr_buf.push(2); arr_buf.extend_from_slice(&n.to_le_bytes()); }
                        Value::Float64(n) => { arr_buf.push(3); arr_buf.extend_from_slice(&n.to_le_bytes()); }
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
            Value::Interval { months, days, microseconds } => {
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
                row.push(Value::Bool(data[pos] != 0));
                pos += 1;
            }
            DataType::Int32 => {
                if pos + 4 > data.len() {
                    return None;
                }
                let n = i32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
                row.push(Value::Int32(n));
                pos += 4;
            }
            DataType::Int64 => {
                if pos + 8 > data.len() {
                    return None;
                }
                let n = i64::from_le_bytes([
                    data[pos], data[pos + 1], data[pos + 2], data[pos + 3],
                    data[pos + 4], data[pos + 5], data[pos + 6], data[pos + 7],
                ]);
                row.push(Value::Int64(n));
                pos += 8;
            }
            DataType::Float64 => {
                if pos + 8 > data.len() {
                    return None;
                }
                let n = f64::from_le_bytes([
                    data[pos], data[pos + 1], data[pos + 2], data[pos + 3],
                    data[pos + 4], data[pos + 5], data[pos + 6], data[pos + 7],
                ]);
                row.push(Value::Float64(n));
                pos += 8;
            }
            DataType::Text => {
                if pos + 4 > data.len() {
                    return None;
                }
                let len = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
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
                let len = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
                pos += 4;
                if pos + len > data.len() {
                    return None;
                }
                let v: serde_json::Value = serde_json::from_slice(&data[pos..pos + len]).ok()?;
                row.push(Value::Jsonb(v));
                pos += len;
            }
            DataType::Date => {
                if pos + 4 > data.len() {
                    return None;
                }
                let d = i32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
                row.push(Value::Date(d));
                pos += 4;
            }
            DataType::Timestamp => {
                if pos + 8 > data.len() {
                    return None;
                }
                let us = i64::from_le_bytes([
                    data[pos], data[pos + 1], data[pos + 2], data[pos + 3],
                    data[pos + 4], data[pos + 5], data[pos + 6], data[pos + 7],
                ]);
                row.push(Value::Timestamp(us));
                pos += 8;
            }
            DataType::TimestampTz => {
                if pos + 8 > data.len() {
                    return None;
                }
                let us = i64::from_le_bytes([
                    data[pos], data[pos + 1], data[pos + 2], data[pos + 3],
                    data[pos + 4], data[pos + 5], data[pos + 6], data[pos + 7],
                ]);
                row.push(Value::TimestampTz(us));
                pos += 8;
            }
            DataType::Numeric => {
                if pos + 4 > data.len() {
                    return None;
                }
                let len = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
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
                let len = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
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
                let total_len = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
                pos += 4;
                if pos + total_len > data.len() {
                    return None;
                }
                let arr_end = pos + total_len;
                // Read elem_count
                if pos + 4 > arr_end {
                    return None;
                }
                let elem_count = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
                pos += 4;
                let mut elems = Vec::with_capacity(elem_count);
                for _ in 0..elem_count {
                    if pos >= arr_end {
                        return None;
                    }
                    let tag = data[pos];
                    pos += 1;
                    match tag {
                        0 => { // Bool
                            if pos >= arr_end { return None; }
                            elems.push(Value::Bool(data[pos] != 0));
                            pos += 1;
                        }
                        1 => { // Int32
                            if pos + 4 > arr_end { return None; }
                            let n = i32::from_le_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]);
                            elems.push(Value::Int32(n));
                            pos += 4;
                        }
                        2 => { // Int64
                            if pos + 8 > arr_end { return None; }
                            let n = i64::from_le_bytes([
                                data[pos], data[pos+1], data[pos+2], data[pos+3],
                                data[pos+4], data[pos+5], data[pos+6], data[pos+7],
                            ]);
                            elems.push(Value::Int64(n));
                            pos += 8;
                        }
                        3 => { // Float64
                            if pos + 8 > arr_end { return None; }
                            let n = f64::from_le_bytes([
                                data[pos], data[pos+1], data[pos+2], data[pos+3],
                                data[pos+4], data[pos+5], data[pos+6], data[pos+7],
                            ]);
                            elems.push(Value::Float64(n));
                            pos += 8;
                        }
                        4 => { // Text
                            if pos + 4 > arr_end { return None; }
                            let slen = u32::from_le_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]) as usize;
                            pos += 4;
                            if pos + slen > arr_end { return None; }
                            let s = std::str::from_utf8(&data[pos..pos + slen]).ok()?;
                            elems.push(Value::Text(s.to_string()));
                            pos += slen;
                        }
                        5 => { // Null
                            elems.push(Value::Null);
                        }
                        _ => return None, // Unknown tag
                    }
                }
                row.push(Value::Array(elems));
                pos = arr_end;
            }
            DataType::Vector(dim) => {
                // Deserialize packed floats
                if pos + 4 > data.len() {
                    return None;
                }
                let count = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
                pos += 4;
                if count != *dim || pos + count * 4 > data.len() {
                    return None;
                }
                let mut vec = Vec::with_capacity(count);
                for _ in 0..count {
                    let f = f32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
                    vec.push(f);
                    pos += 4;
                }
                row.push(Value::Vector(vec));
            }
            DataType::Interval => {
                if pos + 16 > data.len() {
                    return None;
                }
                let months = i32::from_le_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]);
                pos += 4;
                let days = i32::from_le_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]);
                pos += 4;
                let microseconds = i64::from_le_bytes([
                    data[pos], data[pos+1], data[pos+2], data[pos+3],
                    data[pos+4], data[pos+5], data[pos+6], data[pos+7],
                ]);
                pos += 8;
                row.push(Value::Interval { months, days, microseconds });
            }
            DataType::UserDefined(_) => {
                // Enum values stored as length-prefixed UTF-8, same as Text.
                if pos + 4 > data.len() { return None; }
                let len = u32::from_le_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]) as usize;
                pos += 4;
                if pos + len > data.len() { return None; }
                let s = std::str::from_utf8(&data[pos..pos+len]).ok()?.to_string();
                pos += len;
                row.push(Value::Text(s));
            }
        }
    }

    Some(row)
}

#[cfg(test)]
mod tests {
    use super::*;

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
            Value::Date(18000),  // some date
            Value::Timestamp(1700000000000000),
            Value::TimestampTz(1700000000000000),
            Value::Numeric("3.14159".into()),
            Value::Uuid([1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16]),
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
}
