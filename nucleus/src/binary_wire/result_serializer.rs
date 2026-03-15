//! Binary Protocol Result Serializer — row encoding and result formatting.
//!
//! Efficiently encodes result rows for transmission over binary protocol.
//! Supports:
//! - Column metadata (schema information)
//! - Row data serialization (optimized for performance)
//! - Status responses (errors, command completion)
//! - NULL handling

use bytes::{BytesMut, BufMut};
use crate::types::Value;

/// Column metadata for result set.
#[derive(Debug, Clone)]
pub struct ColumnMetadata {
    /// Column name
    pub name: String,
    /// Data type code (matches binary protocol type encoding)
    pub type_code: u8,
    /// Column ID in table (0-based)
    pub column_id: u16,
    /// Flags: null_allowed, etc.
    pub flags: u8,
}

impl ColumnMetadata {
    /// Create new column metadata.
    pub fn new(column_id: u16, name: impl Into<String>, type_code: u8) -> Self {
        Self {
            name: name.into(),
            type_code,
            column_id,
            flags: 0,
        }
    }

    /// Encode column metadata as bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&self.column_id.to_be_bytes());
        buf.extend_from_slice(self.name.as_bytes());
        buf.put_u8(0); // null terminator
        buf.put_u8(self.type_code);
        buf.put_u8(self.flags);
        buf.to_vec()
    }
}

/// Data type code constants for binary protocol.
pub mod type_codes {
    pub const NULL: u8 = 0;
    pub const BOOL: u8 = 1;
    pub const INT32: u8 = 2;
    pub const INT64: u8 = 3;
    pub const FLOAT64: u8 = 4;
    pub const STRING: u8 = 5;
    pub const BYTES: u8 = 6;
    pub const ARRAY: u8 = 7;
    pub const JSON: u8 = 8;
    pub const VECTOR: u8 = 9;
    pub const TIMESTAMP: u8 = 10;
    pub const UUID: u8 = 11;
    pub const DECIMAL: u8 = 12;
    pub const INTERVAL: u8 = 13;
}

/// Result row encoder.
pub struct ResultEncoder {
    buffer: BytesMut,
}

impl ResultEncoder {
    /// Create a new result encoder.
    pub fn new() -> Self {
        Self {
            buffer: BytesMut::with_capacity(4096),
        }
    }

    /// Encode a single row (Vec<Value>).
    pub fn encode_row(&mut self, row: &[Value]) -> Vec<u8> {
        self.buffer.clear();

        // Column count
        self.buffer.extend_from_slice(&(row.len() as u16).to_be_bytes());

        // Each column value
        for value in row {
            self.encode_value(value);
        }

        self.buffer.to_vec()
    }

    /// Encode a single value.
    fn encode_value(&mut self, value: &Value) {
        match value {
            Value::Null => {
                self.buffer.put_u8(type_codes::NULL);
                self.buffer.extend_from_slice(&0u32.to_be_bytes()); // length = 0
            }
            Value::Bool(b) => {
                self.buffer.put_u8(type_codes::BOOL);
                self.buffer.extend_from_slice(&1u32.to_be_bytes());
                self.buffer.put_u8(if *b { 1 } else { 0 });
            }
            Value::Int32(n) => {
                self.buffer.put_u8(type_codes::INT32);
                self.buffer.extend_from_slice(&4u32.to_be_bytes());
                self.buffer.extend_from_slice(&n.to_be_bytes());
            }
            Value::Int64(n) => {
                self.buffer.put_u8(type_codes::INT64);
                self.buffer.extend_from_slice(&8u32.to_be_bytes());
                self.buffer.extend_from_slice(&n.to_be_bytes());
            }
            Value::Float64(f) => {
                self.buffer.put_u8(type_codes::FLOAT64);
                self.buffer.extend_from_slice(&8u32.to_be_bytes());
                self.buffer.extend_from_slice(&f.to_be_bytes());
            }
            Value::Text(s) => {
                self.buffer.put_u8(type_codes::STRING);
                let bytes = s.as_bytes();
                self.buffer.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
                self.buffer.extend_from_slice(bytes);
            }
            _ => {
                // Fallback: encode as JSON using Display trait
                let json_str = value.to_string();
                self.buffer.put_u8(type_codes::JSON);
                let bytes = json_str.as_bytes();
                self.buffer.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
                self.buffer.extend_from_slice(bytes);
            }
        }
    }

    /// Encode command complete message (e.g., "INSERT 0 1").
    pub fn encode_command_complete(&mut self, affected_rows: u32, message: &str) -> Vec<u8> {
        self.buffer.clear();
        self.buffer.extend_from_slice(&affected_rows.to_be_bytes());
        self.buffer.extend_from_slice(message.as_bytes());
        self.buffer.to_vec()
    }

    /// Encode error message.
    pub fn encode_error(&mut self, error_code: u16, message: &str) -> Vec<u8> {
        self.buffer.clear();
        self.buffer.extend_from_slice(&error_code.to_be_bytes());
        self.buffer.extend_from_slice(message.as_bytes());
        self.buffer.to_vec()
    }
}

impl Default for ResultEncoder {
    fn default() -> Self {
        Self::new()
    }
}

/// Result formatter (high-level API).
pub struct ResultFormatter {
    encoder: ResultEncoder,
}

impl ResultFormatter {
    /// Create a new result formatter.
    pub fn new() -> Self {
        Self {
            encoder: ResultEncoder::new(),
        }
    }

    /// Format a complete result set.
    pub fn format_result_set(
        &mut self,
        columns: Vec<ColumnMetadata>,
        rows: Vec<Vec<Value>>,
    ) -> Vec<Vec<u8>> {
        let mut output = Vec::new();

        // Column metadata for each column
        for column in &columns {
            output.push(column.encode());
        }

        // Row data
        for row in rows {
            output.push(self.encoder.encode_row(&row));
        }

        output
    }

    /// Format a single row.
    pub fn format_row(&mut self, row: &[Value]) -> Vec<u8> {
        self.encoder.encode_row(row)
    }
}

impl Default for ResultFormatter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_metadata_creation() {
        let col = ColumnMetadata::new(0, "user_id", type_codes::INT64);
        assert_eq!(col.name, "user_id");
        assert_eq!(col.column_id, 0);
        assert_eq!(col.type_code, type_codes::INT64);
    }

    #[test]
    fn test_column_metadata_encode() {
        let col = ColumnMetadata::new(0, "id", type_codes::INT64);
        let encoded = col.encode();
        assert!(encoded.len() > 0);
        assert_eq!(encoded[0], 0); // column_id high byte
        assert_eq!(encoded[1], 0); // column_id low byte
    }

    #[test]
    fn test_result_encoder_null() {
        let mut encoder = ResultEncoder::new();
        let row = vec![Value::Null];
        let encoded = encoder.encode_row(&row);
        assert_eq!(encoded[0], 0); // column count high byte
        assert_eq!(encoded[1], 1); // column count low byte
        assert_eq!(encoded[2], type_codes::NULL); // value type
    }

    #[test]
    fn test_result_encoder_bool() {
        let mut encoder = ResultEncoder::new();
        let row = vec![Value::Bool(true)];
        let encoded = encoder.encode_row(&row);
        assert_eq!(encoded[2], type_codes::BOOL);
        assert_eq!(encoded[7], 1); // true value
    }

    #[test]
    fn test_result_encoder_int32() {
        let mut encoder = ResultEncoder::new();
        let row = vec![Value::Int32(42)];
        let encoded = encoder.encode_row(&row);
        assert_eq!(encoded[2], type_codes::INT32);
    }

    #[test]
    fn test_result_encoder_int64() {
        let mut encoder = ResultEncoder::new();
        let row = vec![Value::Int64(999999)];
        let encoded = encoder.encode_row(&row);
        assert_eq!(encoded[2], type_codes::INT64);
    }

    #[test]
    fn test_result_encoder_float64() {
        let mut encoder = ResultEncoder::new();
        let row = vec![Value::Float64(3.14)];
        let encoded = encoder.encode_row(&row);
        assert_eq!(encoded[2], type_codes::FLOAT64);
    }

    #[test]
    fn test_result_encoder_string() {
        let mut encoder = ResultEncoder::new();
        let row = vec![Value::Text("hello".to_string())];
        let encoded = encoder.encode_row(&row);
        assert_eq!(encoded[2], type_codes::STRING);
    }

    #[test]
    fn test_result_encoder_multiple_columns() {
        let mut encoder = ResultEncoder::new();
        let row = vec![
            Value::Int64(1),
            Value::Text("user".to_string()),
            Value::Bool(true),
        ];
        let encoded = encoder.encode_row(&row);
        assert_eq!(encoded[1], 3); // 3 columns
    }

    #[test]
    fn test_result_formatter() {
        let mut formatter = ResultFormatter::new();
        let columns = vec![
            ColumnMetadata::new(0, "id", type_codes::INT64),
            ColumnMetadata::new(1, "name", type_codes::STRING),
        ];
        let rows = vec![
            vec![Value::Int64(1), Value::Text("Alice".to_string())],
        ];
        let result = formatter.format_result_set(columns, rows);
        assert_eq!(result.len(), 3); // 2 column metadata + 1 row
    }

    #[test]
    fn test_command_complete_encoding() {
        let mut encoder = ResultEncoder::new();
        let encoded = encoder.encode_command_complete(1, "INSERT 0 1");
        assert!(encoded.len() > 0);
    }

    #[test]
    fn test_error_encoding() {
        let mut encoder = ResultEncoder::new();
        let encoded = encoder.encode_error(1001, "Syntax error");
        assert!(encoded.len() > 0);
    }
}
