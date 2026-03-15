//! Binary protocol message encoding — TLV frame format.
//!
//! Frame structure:
//! ```
//! [type:1byte][length:4bytes Big Endian][payload:N bytes]
//! ```
//!
//! Message types (1-16 defined in spec):
//! - 1: Query (SQL)
//! - 2: PreparedStatement (prepared query)
//! - 3: Bind (parameter binding)
//! - 4: Execute (run query)
//! - 5: CommandComplete (success response)
//! - 6: DataRow (result row)
//! - 7: Error (error response)
//! - 8: Handshake (initial greeting)
//! - 9: Authentication (auth challenge)
//! - 10: Ready (ready for query)
//! - 11: ColumnMetadata (column schema)
//! - 12: ResultEnd (no more rows)
//! - 13: BeginTxn (start transaction)
//! - 14: CommitTxn (commit transaction)
//! - 15: RollbackTxn (rollback transaction)
//! - 16: ParameterStatus (server parameter)

use bytes::{BytesMut, BufMut};

/// Message type constants for binary protocol.
pub mod message_types {
    pub const QUERY: u8 = 1;
    pub const PREPARED_STMT: u8 = 2;
    pub const BIND: u8 = 3;
    pub const EXECUTE: u8 = 4;
    pub const COMMAND_COMPLETE: u8 = 5;
    pub const DATA_ROW: u8 = 6;
    pub const ERROR: u8 = 7;
    pub const HANDSHAKE: u8 = 8;
    pub const AUTHENTICATION: u8 = 9;
    pub const READY: u8 = 10;
    pub const COLUMN_METADATA: u8 = 11;
    pub const RESULT_END: u8 = 12;
    pub const BEGIN_TXN: u8 = 13;
    pub const COMMIT_TXN: u8 = 14;
    pub const ROLLBACK_TXN: u8 = 15;
    pub const PARAMETER_STATUS: u8 = 16;
}

/// Low-level TLV encoder for binary protocol frames.
#[derive(Debug)]
pub struct Encoder {
    buffer: BytesMut,
}

impl Encoder {
    /// Create a new encoder with default capacity.
    pub fn new() -> Self {
        Self {
            buffer: BytesMut::with_capacity(4096),
        }
    }

    /// Create a new encoder with specific capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: BytesMut::with_capacity(capacity),
        }
    }

    /// Reset the encoder buffer for a new message.
    pub fn reset(&mut self) {
        self.buffer.clear();
    }

    /// Get the current buffer contents.
    pub fn buffer(&self) -> &[u8] {
        &self.buffer
    }

    /// Get a mutable reference to the buffer for external writes.
    pub fn buffer_mut(&mut self) -> &mut BytesMut {
        &mut self.buffer
    }

    /// Get the buffer as bytes and clear the encoder.
    pub fn finish(self) -> Vec<u8> {
        self.buffer.to_vec()
    }

    /// Encode a complete TLV frame: [type:1][length:4][payload:N]
    pub fn encode_frame(&mut self, message_type: u8, payload: &[u8]) {
        // Type (1 byte)
        self.buffer.put_u8(message_type);

        // Length (4 bytes, Big Endian)
        self.buffer.put_u32(payload.len() as u32);

        // Payload
        self.buffer.put_slice(payload);
    }

    /// Encode a query message.
    /// Payload: [flags:1][query_id:4][sql:variable length string]
    pub fn encode_query(&mut self, query_id: u32, sql: &str) {
        let mut payload = BytesMut::new();
        payload.put_u8(0); // flags: reserved
        payload.put_u32(query_id);
        payload.put_slice(sql.as_bytes());

        self.encode_frame(message_types::QUERY, &payload);
    }

    /// Encode a prepared statement message.
    /// Payload: [stmt_id:4][sql:variable length string]
    pub fn encode_prepared_stmt(&mut self, stmt_id: u32, sql: &str) {
        let mut payload = BytesMut::new();
        payload.put_u32(stmt_id);
        payload.put_slice(sql.as_bytes());

        self.encode_frame(message_types::PREPARED_STMT, &payload);
    }

    /// Encode a parameter bind message.
    /// Payload: [stmt_id:4][param_count:2][params:variable]
    pub fn encode_bind(&mut self, stmt_id: u32, param_count: u16, params_bytes: &[u8]) {
        let mut payload = BytesMut::new();
        payload.put_u32(stmt_id);
        payload.put_u16(param_count);
        payload.put_slice(params_bytes);

        self.encode_frame(message_types::BIND, &payload);
    }

    /// Encode an execute message.
    /// Payload: [stmt_id:4][flags:1]
    pub fn encode_execute(&mut self, stmt_id: u32) {
        let mut payload = BytesMut::new();
        payload.put_u32(stmt_id);
        payload.put_u8(0); // flags: reserved

        self.encode_frame(message_types::EXECUTE, &payload);
    }

    /// Encode a command complete message (success).
    /// Payload: [affected_rows:4][message:variable length string]
    pub fn encode_command_complete(&mut self, affected_rows: u32, message: &str) {
        let mut payload = BytesMut::new();
        payload.put_u32(affected_rows);
        payload.put_slice(message.as_bytes());

        self.encode_frame(message_types::COMMAND_COMPLETE, &payload);
    }

    /// Encode a data row message.
    /// Payload: [column_count:2][columns:variable]
    pub fn encode_data_row(&mut self, column_count: u16, columns_bytes: &[u8]) {
        let mut payload = BytesMut::new();
        payload.put_u16(column_count);
        payload.put_slice(columns_bytes);

        self.encode_frame(message_types::DATA_ROW, &payload);
    }

    /// Encode an error message.
    /// Payload: [error_code:2][message:variable length string]
    pub fn encode_error(&mut self, error_code: u16, message: &str) {
        let mut payload = BytesMut::new();
        payload.put_u16(error_code);
        payload.put_slice(message.as_bytes());

        self.encode_frame(message_types::ERROR, &payload);
    }

    /// Encode a handshake message.
    /// Payload: [version:4][server_id:4][flags:1]
    pub fn encode_handshake(&mut self, version: u32, server_id: u32) {
        let mut payload = BytesMut::new();
        payload.put_u32(version);
        payload.put_u32(server_id);
        payload.put_u8(0); // flags: reserved

        self.encode_frame(message_types::HANDSHAKE, &payload);
    }

    /// Encode a ready for query message.
    /// Payload: [status:1] (0=idle, 1=in_txn, 2=error)
    pub fn encode_ready(&mut self, status: u8) {
        self.encode_frame(message_types::READY, &[status]);
    }

    /// Encode column metadata message.
    /// Payload: [column_id:2][name:variable][type:1][flags:1]
    pub fn encode_column_metadata(&mut self, column_id: u16, name: &str, type_code: u8) {
        let mut payload = BytesMut::new();
        payload.put_u16(column_id);
        payload.put_slice(name.as_bytes());
        payload.put_u8(0); // null terminator
        payload.put_u8(type_code);
        payload.put_u8(0); // flags: reserved

        self.encode_frame(message_types::COLUMN_METADATA, &payload);
    }

    /// Encode a result end message (no more rows).
    /// Payload: [row_count:4]
    pub fn encode_result_end(&mut self, row_count: u32) {
        let mut payload = BytesMut::new();
        payload.put_u32(row_count);

        self.encode_frame(message_types::RESULT_END, &payload);
    }

    /// Encode a begin transaction message.
    /// Payload: [isolation_level:1]
    pub fn encode_begin_txn(&mut self, isolation_level: u8) {
        self.encode_frame(message_types::BEGIN_TXN, &[isolation_level]);
    }

    /// Encode a commit transaction message.
    /// Payload: (empty)
    pub fn encode_commit_txn(&mut self) {
        self.encode_frame(message_types::COMMIT_TXN, &[]);
    }

    /// Encode a rollback transaction message.
    /// Payload: (empty)
    pub fn encode_rollback_txn(&mut self) {
        self.encode_frame(message_types::ROLLBACK_TXN, &[]);
    }

    /// Encode a parameter status message.
    /// Payload: [param_name:variable][param_value:variable]
    pub fn encode_parameter_status(&mut self, name: &str, value: &str) {
        let mut payload = BytesMut::new();
        payload.put_slice(name.as_bytes());
        payload.put_u8(0); // null terminator
        payload.put_slice(value.as_bytes());

        self.encode_frame(message_types::PARAMETER_STATUS, &payload);
    }
}

impl Default for Encoder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encoder_creation() {
        let encoder = Encoder::new();
        assert_eq!(encoder.buffer().len(), 0);
    }

    #[test]
    fn test_encode_frame_basic() {
        let mut encoder = Encoder::new();
        encoder.encode_frame(message_types::QUERY, b"SELECT 1");
        let result = encoder.buffer();
        assert_eq!(result[0], message_types::QUERY);
        assert_eq!(u32::from_be_bytes([result[1], result[2], result[3], result[4]]), 8);
        assert_eq!(&result[5..], b"SELECT 1");
    }

    #[test]
    fn test_encode_query() {
        let mut encoder = Encoder::new();
        encoder.encode_query(42, "SELECT * FROM users");
        let result = encoder.buffer();
        assert_eq!(result[0], message_types::QUERY);
        // length = 1 (flags) + 4 (query_id) + 19 (sql "SELECT * FROM users") = 24
        let len = u32::from_be_bytes([result[1], result[2], result[3], result[4]]);
        assert_eq!(len, 24);
    }

    #[test]
    fn test_encode_error() {
        let mut encoder = Encoder::new();
        encoder.encode_error(1001, "Syntax error");
        let result = encoder.buffer();
        assert_eq!(result[0], message_types::ERROR);
        // Payload starts at offset 5: [error_code:2][message:variable]
        assert_eq!(u16::from_be_bytes([result[5], result[6]]), 1001);
    }

    #[test]
    fn test_encode_handshake() {
        let mut encoder = Encoder::new();
        encoder.encode_handshake(1, 12345);
        let result = encoder.buffer();
        assert_eq!(result[0], message_types::HANDSHAKE);
        assert_eq!(u32::from_be_bytes([result[1], result[2], result[3], result[4]]), 9);
    }

    #[test]
    fn test_encode_ready() {
        let mut encoder = Encoder::new();
        encoder.encode_ready(0); // idle
        let result = encoder.buffer();
        assert_eq!(result[0], message_types::READY);
        assert_eq!(result[5], 0);
    }

    #[test]
    fn test_encode_command_complete() {
        let mut encoder = Encoder::new();
        encoder.encode_command_complete(1, "INSERT 0 1");
        let result = encoder.buffer();
        assert_eq!(result[0], message_types::COMMAND_COMPLETE);
        // Payload starts at offset 5: [affected_rows:4][message:variable]
        let affected = u32::from_be_bytes([result[5], result[6], result[7], result[8]]);
        assert_eq!(affected, 1);
    }

    #[test]
    fn test_multiple_frames() {
        let mut encoder = Encoder::new();
        encoder.encode_ready(0);
        let first = encoder.buffer().len();
        encoder.encode_query(1, "SELECT 1");
        let total = encoder.buffer().len();
        assert!(total > first);
    }

    #[test]
    fn test_encoder_reset() {
        let mut encoder = Encoder::new();
        encoder.encode_query(1, "SELECT 1");
        assert!(encoder.buffer().len() > 0);
        encoder.reset();
        assert_eq!(encoder.buffer().len(), 0);
    }

    #[test]
    fn test_encode_prepared_stmt() {
        let mut encoder = Encoder::new();
        encoder.encode_prepared_stmt(99, "INSERT INTO t VALUES (?)");
        let result = encoder.buffer();
        assert_eq!(result[0], message_types::PREPARED_STMT);
    }

    #[test]
    fn test_encode_result_end() {
        let mut encoder = Encoder::new();
        encoder.encode_result_end(42);
        let result = encoder.buffer();
        assert_eq!(result[0], message_types::RESULT_END);
        assert_eq!(u32::from_be_bytes([result[1], result[2], result[3], result[4]]), 4);
    }

    #[test]
    fn test_encode_parameter_status() {
        let mut encoder = Encoder::new();
        encoder.encode_parameter_status("application_name", "nucleus");
        let result = encoder.buffer();
        assert_eq!(result[0], message_types::PARAMETER_STATUS);
    }
}
