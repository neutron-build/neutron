//! Binary protocol message decoding — TLV frame parsing.
//!
//! Handles incoming data stream, parses TLV frames with error recovery,
//! and supports streaming (incomplete frame buffering).
//!
//! Frame structure:
//! ```
//! [type:1byte][length:4bytes Big Endian][payload:N bytes]
//! ```

use bytes::{BytesMut, Buf};
use std::fmt;

pub use super::encoder::message_types;

/// Errors that can occur during frame decoding.
#[derive(Debug, Clone, PartialEq)]
pub enum DecodeError {
    /// Invalid or unrecognized message type.
    InvalidFrameType(u8),
    /// Frame length exceeds maximum allowed size.
    InvalidLength(u32),
    /// Incomplete frame — not enough data yet (streaming).
    IncompleteFrame,
    /// Malformed payload data.
    InvalidPayload(String),
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DecodeError::InvalidFrameType(t) => write!(f, "Invalid frame type: {}", t),
            DecodeError::InvalidLength(len) => write!(f, "Invalid frame length: {}", len),
            DecodeError::IncompleteFrame => write!(f, "Incomplete frame — waiting for more data"),
            DecodeError::InvalidPayload(msg) => write!(f, "Invalid payload: {}", msg),
        }
    }
}

impl std::error::Error for DecodeError {}

/// Decoded frame with message type and payload reference.
#[derive(Debug, Clone, PartialEq)]
pub struct DecodedFrame {
    pub message_type: u8,
    pub payload: Vec<u8>,
}

/// Maximum allowed frame size (256 MB).
const MAX_FRAME_SIZE: u32 = 256 * 1024 * 1024;

/// Minimum frame header size (type:1 + length:4).
const FRAME_HEADER_SIZE: usize = 5;

/// Low-level TLV decoder for binary protocol frames.
/// Handles streaming input with buffering.
pub struct Decoder {
    buffer: BytesMut,
}

impl Decoder {
    /// Create a new decoder with default capacity.
    pub fn new() -> Self {
        Self {
            buffer: BytesMut::with_capacity(4096),
        }
    }

    /// Create a new decoder with specific capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: BytesMut::with_capacity(capacity),
        }
    }

    /// Feed incoming bytes into the decoder buffer.
    pub fn feed(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    /// Parse the next complete frame from the buffer.
    /// Returns `Ok(None)` if frame is incomplete (need more data).
    /// Returns `Ok(Some(frame))` if a complete frame was parsed.
    /// Returns `Err` if the frame is malformed.
    pub fn parse_frame(&mut self) -> Result<Option<DecodedFrame>, DecodeError> {
        // Need at least the header (type:1 + length:4)
        if self.buffer.len() < FRAME_HEADER_SIZE {
            return Ok(None); // Incomplete frame, need more data
        }

        // Peek at the frame header without consuming yet
        let header_slice = &self.buffer[0..FRAME_HEADER_SIZE];
        let message_type = header_slice[0];
        let frame_length = u32::from_be_bytes([
            header_slice[1],
            header_slice[2],
            header_slice[3],
            header_slice[4],
        ]);

        // Validate message type
        if !is_valid_message_type(message_type) {
            return Err(DecodeError::InvalidFrameType(message_type));
        }

        // Validate frame length
        if frame_length > MAX_FRAME_SIZE {
            return Err(DecodeError::InvalidLength(frame_length));
        }

        // Check if we have the complete frame (header + payload)
        let total_frame_size = FRAME_HEADER_SIZE + frame_length as usize;
        if self.buffer.len() < total_frame_size {
            return Ok(None); // Incomplete frame, need more data
        }

        // Extract the complete frame
        let frame_slice = &self.buffer[0..total_frame_size];
        let payload = frame_slice[FRAME_HEADER_SIZE..].to_vec();

        // Consume the frame from the buffer
        self.buffer.advance(total_frame_size);

        Ok(Some(DecodedFrame {
            message_type,
            payload,
        }))
    }

    /// Get the number of unconsumed bytes in the buffer.
    pub fn remaining(&self) -> usize {
        self.buffer.len()
    }

    /// Clear the buffer and reset the decoder.
    pub fn reset(&mut self) {
        self.buffer.clear();
    }

    /// Parse a Query message from decoded frame payload.
    /// Payload: [flags:1][query_id:4][sql:variable]
    pub fn parse_query(payload: &[u8]) -> Result<(u8, u32, &str), DecodeError> {
        if payload.len() < 5 {
            return Err(DecodeError::InvalidPayload(
                "Query payload too short".to_string(),
            ));
        }

        let flags = payload[0];
        let query_id = u32::from_be_bytes([payload[1], payload[2], payload[3], payload[4]]);
        let sql_bytes = &payload[5..];
        let sql = std::str::from_utf8(sql_bytes)
            .map_err(|_| DecodeError::InvalidPayload("Invalid UTF-8 in SQL".to_string()))?;

        Ok((flags, query_id, sql))
    }

    /// Parse a PreparedStatement message.
    /// Payload: [stmt_id:4][sql:variable]
    pub fn parse_prepared_stmt(payload: &[u8]) -> Result<(u32, &str), DecodeError> {
        if payload.len() < 4 {
            return Err(DecodeError::InvalidPayload(
                "PreparedStatement payload too short".to_string(),
            ));
        }

        let stmt_id = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
        let sql_bytes = &payload[4..];
        let sql = std::str::from_utf8(sql_bytes)
            .map_err(|_| DecodeError::InvalidPayload("Invalid UTF-8 in SQL".to_string()))?;

        Ok((stmt_id, sql))
    }

    /// Parse a Bind message.
    /// Payload: [stmt_id:4][param_count:2][params:variable]
    pub fn parse_bind(payload: &[u8]) -> Result<(u32, u16, &[u8]), DecodeError> {
        if payload.len() < 6 {
            return Err(DecodeError::InvalidPayload(
                "Bind payload too short".to_string(),
            ));
        }

        let stmt_id = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
        let param_count = u16::from_be_bytes([payload[4], payload[5]]);
        let params_bytes = &payload[6..];

        Ok((stmt_id, param_count, params_bytes))
    }

    /// Parse an Execute message.
    /// Payload: [stmt_id:4][flags:1]
    pub fn parse_execute(payload: &[u8]) -> Result<(u32, u8), DecodeError> {
        if payload.len() < 5 {
            return Err(DecodeError::InvalidPayload(
                "Execute payload too short".to_string(),
            ));
        }

        let stmt_id = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
        let flags = payload[4];

        Ok((stmt_id, flags))
    }

    /// Parse a CommandComplete message.
    /// Payload: [affected_rows:4][message:variable]
    pub fn parse_command_complete(payload: &[u8]) -> Result<(u32, &str), DecodeError> {
        if payload.len() < 4 {
            return Err(DecodeError::InvalidPayload(
                "CommandComplete payload too short".to_string(),
            ));
        }

        let affected_rows = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
        let message_bytes = &payload[4..];
        let message = std::str::from_utf8(message_bytes).map_err(|_| {
            DecodeError::InvalidPayload("Invalid UTF-8 in message".to_string())
        })?;

        Ok((affected_rows, message))
    }

    /// Parse a DataRow message.
    /// Payload: [column_count:2][columns:variable]
    pub fn parse_data_row(payload: &[u8]) -> Result<(u16, &[u8]), DecodeError> {
        if payload.len() < 2 {
            return Err(DecodeError::InvalidPayload(
                "DataRow payload too short".to_string(),
            ));
        }

        let column_count = u16::from_be_bytes([payload[0], payload[1]]);
        let columns_bytes = &payload[2..];

        Ok((column_count, columns_bytes))
    }

    /// Parse an Error message.
    /// Payload: [error_code:2][message:variable]
    pub fn parse_error(payload: &[u8]) -> Result<(u16, &str), DecodeError> {
        if payload.len() < 2 {
            return Err(DecodeError::InvalidPayload(
                "Error payload too short".to_string(),
            ));
        }

        let error_code = u16::from_be_bytes([payload[0], payload[1]]);
        let message_bytes = &payload[2..];
        let message = std::str::from_utf8(message_bytes).map_err(|_| {
            DecodeError::InvalidPayload("Invalid UTF-8 in error message".to_string())
        })?;

        Ok((error_code, message))
    }

    /// Parse a Handshake message.
    /// Payload: [version:4][server_id:4][flags:1]
    pub fn parse_handshake(payload: &[u8]) -> Result<(u32, u32, u8), DecodeError> {
        if payload.len() < 9 {
            return Err(DecodeError::InvalidPayload(
                "Handshake payload too short".to_string(),
            ));
        }

        let version = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
        let server_id = u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]);
        let flags = payload[8];

        Ok((version, server_id, flags))
    }

    /// Parse an Authentication message.
    /// Payload: [auth_type:1][auth_data:variable]
    pub fn parse_authentication(payload: &[u8]) -> Result<(u8, &[u8]), DecodeError> {
        if payload.is_empty() {
            return Err(DecodeError::InvalidPayload(
                "Authentication payload empty".to_string(),
            ));
        }

        let auth_type = payload[0];
        let auth_data = &payload[1..];

        Ok((auth_type, auth_data))
    }

    /// Parse a Ready message.
    /// Payload: [status:1] (0=idle, 1=in_txn, 2=error)
    pub fn parse_ready(payload: &[u8]) -> Result<u8, DecodeError> {
        if payload.len() < 1 {
            return Err(DecodeError::InvalidPayload(
                "Ready payload too short".to_string(),
            ));
        }

        Ok(payload[0])
    }

    /// Parse ColumnMetadata message.
    /// Payload: [column_id:2][name:variable][null_terminator:1][type:1][flags:1]
    pub fn parse_column_metadata(payload: &[u8]) -> Result<(u16, &str, u8, u8), DecodeError> {
        if payload.len() < 5 {
            return Err(DecodeError::InvalidPayload(
                "ColumnMetadata payload too short".to_string(),
            ));
        }

        let column_id = u16::from_be_bytes([payload[0], payload[1]]);

        // Find null terminator
        let name_end = payload[2..]
            .iter()
            .position(|&b| b == 0)
            .ok_or_else(|| {
                DecodeError::InvalidPayload("No null terminator in column name".to_string())
            })?;

        let name_bytes = &payload[2..2 + name_end];
        let name = std::str::from_utf8(name_bytes).map_err(|_| {
            DecodeError::InvalidPayload("Invalid UTF-8 in column name".to_string())
        })?;

        let type_offset = 2 + name_end + 1;
        if payload.len() < type_offset + 2 {
            return Err(DecodeError::InvalidPayload(
                "ColumnMetadata payload truncated".to_string(),
            ));
        }

        let type_code = payload[type_offset];
        let flags = payload[type_offset + 1];

        Ok((column_id, name, type_code, flags))
    }

    /// Parse a ResultEnd message.
    /// Payload: [row_count:4]
    pub fn parse_result_end(payload: &[u8]) -> Result<u32, DecodeError> {
        if payload.len() < 4 {
            return Err(DecodeError::InvalidPayload(
                "ResultEnd payload too short".to_string(),
            ));
        }

        Ok(u32::from_be_bytes([
            payload[0],
            payload[1],
            payload[2],
            payload[3],
        ]))
    }

    /// Parse a BeginTxn message.
    /// Payload: [isolation_level:1]
    pub fn parse_begin_txn(payload: &[u8]) -> Result<u8, DecodeError> {
        if payload.len() < 1 {
            return Err(DecodeError::InvalidPayload(
                "BeginTxn payload too short".to_string(),
            ));
        }

        Ok(payload[0])
    }

    /// Parse a CommitTxn message.
    /// Payload: (empty)
    pub fn parse_commit_txn(payload: &[u8]) -> Result<(), DecodeError> {
        if !payload.is_empty() {
            return Err(DecodeError::InvalidPayload(
                "CommitTxn payload should be empty".to_string(),
            ));
        }

        Ok(())
    }

    /// Parse a RollbackTxn message.
    /// Payload: (empty)
    pub fn parse_rollback_txn(payload: &[u8]) -> Result<(), DecodeError> {
        if !payload.is_empty() {
            return Err(DecodeError::InvalidPayload(
                "RollbackTxn payload should be empty".to_string(),
            ));
        }

        Ok(())
    }

    /// Parse a ParameterStatus message.
    /// Payload: [param_name:variable][null_terminator:1][param_value:variable]
    pub fn parse_parameter_status(payload: &[u8]) -> Result<(&str, &str), DecodeError> {
        // Find first null terminator
        let name_end = payload.iter().position(|&b| b == 0).ok_or_else(|| {
            DecodeError::InvalidPayload("No null terminator in parameter name".to_string())
        })?;

        let name_bytes = &payload[0..name_end];
        let name = std::str::from_utf8(name_bytes).map_err(|_| {
            DecodeError::InvalidPayload("Invalid UTF-8 in parameter name".to_string())
        })?;

        let value_bytes = &payload[name_end + 1..];
        let value = std::str::from_utf8(value_bytes).map_err(|_| {
            DecodeError::InvalidPayload("Invalid UTF-8 in parameter value".to_string())
        })?;

        Ok((name, value))
    }
}

impl Default for Decoder {
    fn default() -> Self {
        Self::new()
    }
}

/// Check if a message type is valid (1-16).
fn is_valid_message_type(msg_type: u8) -> bool {
    (1..=16).contains(&msg_type)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binary_wire::encoder::Encoder;

    #[test]
    fn test_decoder_creation() {
        let decoder = Decoder::new();
        assert_eq!(decoder.remaining(), 0);
    }

    #[test]
    fn test_parse_incomplete_frame() {
        let mut decoder = Decoder::new();
        decoder.feed(&[1, 0, 0, 0]); // Only 4 bytes, need 5 for header
        let result = decoder.parse_frame();
        assert_eq!(result, Ok(None));
        assert_eq!(decoder.remaining(), 4);
    }

    #[test]
    fn test_parse_incomplete_payload() {
        let mut decoder = Decoder::new();
        // Header: type=1, length=10
        decoder.feed(&[1, 0, 0, 0, 10]);
        // Only 2 bytes of payload, need 10
        decoder.feed(&[0, 1]);
        let result = decoder.parse_frame();
        assert_eq!(result, Ok(None));
        assert_eq!(decoder.remaining(), 7); // 5 + 2
    }

    #[test]
    fn test_parse_complete_frame() {
        let mut decoder = Decoder::new();
        // Header: type=1, length=8
        decoder.feed(&[1, 0, 0, 0, 8]);
        // Payload: "SELECT 1" (8 bytes)
        decoder.feed(b"SELECT 1");

        let result = decoder.parse_frame();
        assert!(result.is_ok());
        let frame = result.unwrap().unwrap();
        assert_eq!(frame.message_type, 1);
        assert_eq!(frame.payload, b"SELECT 1");
        assert_eq!(decoder.remaining(), 0);
    }

    #[test]
    fn test_invalid_frame_type() {
        let mut decoder = Decoder::new();
        // Invalid type: 99
        decoder.feed(&[99, 0, 0, 0, 0]);
        let result = decoder.parse_frame();
        assert!(matches!(result, Err(DecodeError::InvalidFrameType(99))));
    }

    #[test]
    fn test_frame_too_large() {
        let mut decoder = Decoder::new();
        // Header with length > MAX_FRAME_SIZE
        let _huge_len = (MAX_FRAME_SIZE as u64 + 1) as u32;
        decoder.feed(&[1, 0xFF, 0xFF, 0xFF, 0xFF]);
        let result = decoder.parse_frame();
        assert!(matches!(result, Err(DecodeError::InvalidLength(_))));
    }

    #[test]
    fn test_round_trip_query() {
        let mut encoder = Encoder::new();
        encoder.encode_query(42, "SELECT * FROM users");
        let encoded = encoder.buffer().to_vec();

        let mut decoder = Decoder::new();
        decoder.feed(&encoded);
        let frame = decoder.parse_frame().unwrap().unwrap();

        assert_eq!(frame.message_type, message_types::QUERY);
        let (flags, query_id, sql) = Decoder::parse_query(&frame.payload).unwrap();
        assert_eq!(flags, 0);
        assert_eq!(query_id, 42);
        assert_eq!(sql, "SELECT * FROM users");
    }

    #[test]
    fn test_round_trip_error() {
        let mut encoder = Encoder::new();
        encoder.encode_error(1001, "Syntax error");
        let encoded = encoder.buffer().to_vec();

        let mut decoder = Decoder::new();
        decoder.feed(&encoded);
        let frame = decoder.parse_frame().unwrap().unwrap();

        assert_eq!(frame.message_type, message_types::ERROR);
        let (error_code, message) = Decoder::parse_error(&frame.payload).unwrap();
        assert_eq!(error_code, 1001);
        assert_eq!(message, "Syntax error");
    }

    #[test]
    fn test_round_trip_handshake() {
        let mut encoder = Encoder::new();
        encoder.encode_handshake(1, 12345);
        let encoded = encoder.buffer().to_vec();

        let mut decoder = Decoder::new();
        decoder.feed(&encoded);
        let frame = decoder.parse_frame().unwrap().unwrap();

        assert_eq!(frame.message_type, message_types::HANDSHAKE);
        let (version, server_id, flags) = Decoder::parse_handshake(&frame.payload).unwrap();
        assert_eq!(version, 1);
        assert_eq!(server_id, 12345);
        assert_eq!(flags, 0);
    }

    #[test]
    fn test_round_trip_prepared_stmt() {
        let mut encoder = Encoder::new();
        encoder.encode_prepared_stmt(99, "INSERT INTO t VALUES (?)");
        let encoded = encoder.buffer().to_vec();

        let mut decoder = Decoder::new();
        decoder.feed(&encoded);
        let frame = decoder.parse_frame().unwrap().unwrap();

        assert_eq!(frame.message_type, message_types::PREPARED_STMT);
        let (stmt_id, sql) = Decoder::parse_prepared_stmt(&frame.payload).unwrap();
        assert_eq!(stmt_id, 99);
        assert_eq!(sql, "INSERT INTO t VALUES (?)");
    }

    #[test]
    fn test_round_trip_execute() {
        let mut encoder = Encoder::new();
        encoder.encode_execute(77);
        let encoded = encoder.buffer().to_vec();

        let mut decoder = Decoder::new();
        decoder.feed(&encoded);
        let frame = decoder.parse_frame().unwrap().unwrap();

        assert_eq!(frame.message_type, message_types::EXECUTE);
        let (stmt_id, flags) = Decoder::parse_execute(&frame.payload).unwrap();
        assert_eq!(stmt_id, 77);
        assert_eq!(flags, 0);
    }

    #[test]
    fn test_round_trip_ready() {
        let mut encoder = Encoder::new();
        encoder.encode_ready(1); // in_txn
        let encoded = encoder.buffer().to_vec();

        let mut decoder = Decoder::new();
        decoder.feed(&encoded);
        let frame = decoder.parse_frame().unwrap().unwrap();

        assert_eq!(frame.message_type, message_types::READY);
        let status = Decoder::parse_ready(&frame.payload).unwrap();
        assert_eq!(status, 1);
    }

    #[test]
    fn test_round_trip_command_complete() {
        let mut encoder = Encoder::new();
        encoder.encode_command_complete(5, "INSERT 0 5");
        let encoded = encoder.buffer().to_vec();

        let mut decoder = Decoder::new();
        decoder.feed(&encoded);
        let frame = decoder.parse_frame().unwrap().unwrap();

        assert_eq!(frame.message_type, message_types::COMMAND_COMPLETE);
        let (affected_rows, message) = Decoder::parse_command_complete(&frame.payload).unwrap();
        assert_eq!(affected_rows, 5);
        assert_eq!(message, "INSERT 0 5");
    }

    #[test]
    fn test_round_trip_result_end() {
        let mut encoder = Encoder::new();
        encoder.encode_result_end(42);
        let encoded = encoder.buffer().to_vec();

        let mut decoder = Decoder::new();
        decoder.feed(&encoded);
        let frame = decoder.parse_frame().unwrap().unwrap();

        assert_eq!(frame.message_type, message_types::RESULT_END);
        let row_count = Decoder::parse_result_end(&frame.payload).unwrap();
        assert_eq!(row_count, 42);
    }

    #[test]
    fn test_multiple_frames_sequential() {
        let mut encoder = Encoder::new();
        encoder.encode_ready(0);
        encoder.encode_query(1, "SELECT 1");

        let encoded = encoder.buffer().to_vec();
        let mut decoder = Decoder::new();
        decoder.feed(&encoded);

        // Parse first frame
        let frame1 = decoder.parse_frame().unwrap().unwrap();
        assert_eq!(frame1.message_type, message_types::READY);

        // Parse second frame
        let frame2 = decoder.parse_frame().unwrap().unwrap();
        assert_eq!(frame2.message_type, message_types::QUERY);

        // No more frames
        assert_eq!(decoder.parse_frame().unwrap(), None);
    }

    #[test]
    fn test_chunked_input() {
        let mut encoder = Encoder::new();
        encoder.encode_query(99, "SELECT * FROM data");
        let encoded = encoder.buffer().to_vec();

        let mut decoder = Decoder::new();

        // Feed frame in small chunks
        for chunk in encoded.chunks(3) {
            decoder.feed(chunk);

            // Try to parse each chunk
            if let Ok(Some(_frame)) = decoder.parse_frame() {
                break; // Got the frame
            }
        }

        // Should have successfully parsed
        assert_eq!(decoder.remaining(), 0);
    }

    #[test]
    fn test_parse_query_payload() {
        let payload = [0u8, 0, 0, 0, 42]; // flags=0, query_id=42
        let mut payload_vec = payload.to_vec();
        payload_vec.extend_from_slice(b"SELECT 1");

        let (flags, query_id, sql) = Decoder::parse_query(&payload_vec).unwrap();
        assert_eq!(flags, 0);
        assert_eq!(query_id, 42);
        assert_eq!(sql, "SELECT 1");
    }

    #[test]
    fn test_parse_error_payload() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&[3, 233u8]); // error_code=1001 in BE
        payload.extend_from_slice(b"Invalid syntax");

        let (error_code, message) = Decoder::parse_error(&payload).unwrap();
        assert_eq!(error_code, 1001);
        assert_eq!(message, "Invalid syntax");
    }

    #[test]
    fn test_invalid_utf8() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&[0, 0, 0, 0, 5]); // query_id=5
        payload.extend_from_slice(&[0xFF, 0xFE]); // Invalid UTF-8

        let result = Decoder::parse_query(&payload);
        assert!(matches!(result, Err(DecodeError::InvalidPayload(_))));
    }

    #[test]
    fn test_decoder_reset() {
        let mut decoder = Decoder::new();
        decoder.feed(b"some data");
        assert_eq!(decoder.remaining(), 9);
        decoder.reset();
        assert_eq!(decoder.remaining(), 0);
    }

    #[test]
    fn test_parameter_status_round_trip() {
        let mut encoder = Encoder::new();
        encoder.encode_parameter_status("client_encoding", "UTF8");
        let encoded = encoder.buffer().to_vec();

        let mut decoder = Decoder::new();
        decoder.feed(&encoded);
        let frame = decoder.parse_frame().unwrap().unwrap();

        assert_eq!(frame.message_type, message_types::PARAMETER_STATUS);
        let (name, value) = Decoder::parse_parameter_status(&frame.payload).unwrap();
        assert_eq!(name, "client_encoding");
        assert_eq!(value, "UTF8");
    }

    #[test]
    fn test_begin_txn_round_trip() {
        let mut encoder = Encoder::new();
        encoder.encode_begin_txn(0); // isolation_level=0
        let encoded = encoder.buffer().to_vec();

        let mut decoder = Decoder::new();
        decoder.feed(&encoded);
        let frame = decoder.parse_frame().unwrap().unwrap();

        assert_eq!(frame.message_type, message_types::BEGIN_TXN);
        let isolation_level = Decoder::parse_begin_txn(&frame.payload).unwrap();
        assert_eq!(isolation_level, 0);
    }

    #[test]
    fn test_commit_txn_round_trip() {
        let mut encoder = Encoder::new();
        encoder.encode_commit_txn();
        let encoded = encoder.buffer().to_vec();

        let mut decoder = Decoder::new();
        decoder.feed(&encoded);
        let frame = decoder.parse_frame().unwrap().unwrap();

        assert_eq!(frame.message_type, message_types::COMMIT_TXN);
        let result = Decoder::parse_commit_txn(&frame.payload);
        assert!(result.is_ok());
    }
}
