//! Binary protocol connection handler.
//!
//! Manages a single client connection through its lifecycle:
//! 1. Handshake (version negotiation, authentication)
//! 2. Message dispatch loop (query, prepared statement, transaction)
//! 3. Clean shutdown
//!
//! Each connection gets its own session ID on the executor, matching how
//! pgwire connections work.

use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::executor::{ExecResult, Executor};
use crate::pool::connection_budget::ConnectionBudget;
use crate::types::DataType;

use super::decoder::{DecodedFrame, Decoder, message_types};
use super::encoder::Encoder;
use super::handshake::{HandshakeHandler, ServerParameters};
use super::query_handler::QueryHandler;
use super::result_serializer::{ResultEncoder, type_codes};

/// Error codes for binary protocol error responses.
pub mod error_codes {
    pub const SYNTAX_ERROR: u16 = 4201;
    pub const RUNTIME_ERROR: u16 = 5000;
    pub const INTERNAL_ERROR: u16 = 5001;
    pub const AUTH_FAILED: u16 = 2800;
    pub const PROTOCOL_ERROR: u16 = 8000;
    pub const TRANSACTION_ERROR: u16 = 4000;
}

/// Ready status values.
mod ready_status {
    pub const IDLE: u8 = 0;
    pub const IN_TXN: u8 = 1;
    pub const ERROR: u8 = 2;
}

/// Manages one binary protocol connection.
pub struct ConnectionHandler {
    stream: TcpStream,
    executor: Arc<Executor>,
    session_id: u64,
    decoder: Decoder,
    encoder: Encoder,
    result_encoder: ResultEncoder,
    query_handler: QueryHandler,
    password: Option<String>,
    in_transaction: bool,
    txn_error: bool,
}

impl ConnectionHandler {
    /// Create a new connection handler.
    pub fn new(
        stream: TcpStream,
        executor: Arc<Executor>,
        password: Option<String>,
    ) -> Self {
        let session_id = executor.create_session();
        let query_handler = QueryHandler::new(executor.clone());
        Self {
            stream,
            executor,
            session_id,
            decoder: Decoder::new(),
            encoder: Encoder::new(),
            result_encoder: ResultEncoder::new(),
            query_handler,
            password,
            in_transaction: false,
            txn_error: false,
        }
    }

    /// Run the connection handler: handshake then message loop.
    pub async fn run(&mut self) -> std::io::Result<()> {
        if let Err(e) = self.perform_handshake().await {
            tracing::debug!("Binary protocol handshake failed: {e}");
            return Ok(());
        }

        if let Err(e) = self.message_loop().await {
            tracing::debug!("Binary protocol connection error: {e}");
        }

        self.executor.drop_session(self.session_id);
        Ok(())
    }

    /// Perform the handshake sequence.
    async fn perform_handshake(&mut self) -> Result<(), String> {
        let budget = ConnectionBudget::new();
        let mut handshake = HandshakeHandler::new(budget);

        // Read client handshake message
        let frame = self.read_frame().await.map_err(|e| e.to_string())?;
        let frame = frame.ok_or_else(|| "connection closed during handshake".to_string())?;

        if frame.message_type != message_types::HANDSHAKE {
            return Err(format!(
                "expected handshake message, got type {}",
                frame.message_type
            ));
        }

        // Process client handshake
        let server_response = handshake.handle_client_handshake(&frame.payload)?;
        self.stream
            .write_all(&server_response)
            .await
            .map_err(|e| e.to_string())?;

        // Send auth challenge
        let challenge_msg = handshake.send_auth_challenge()?;
        self.stream
            .write_all(&challenge_msg)
            .await
            .map_err(|e| e.to_string())?;

        // Read auth response
        let auth_frame = self.read_frame().await.map_err(|e| e.to_string())?;
        let auth_frame =
            auth_frame.ok_or_else(|| "connection closed during auth".to_string())?;

        if auth_frame.message_type != message_types::AUTHENTICATION {
            return Err(format!(
                "expected auth response, got type {}",
                auth_frame.message_type
            ));
        }

        // Validate authentication
        let expected_pw = self.password.as_deref().unwrap_or("");
        let auth_ok = handshake.handle_auth_response(&auth_frame.payload, expected_pw)?;

        if !auth_ok {
            self.send_error(error_codes::AUTH_FAILED, "authentication failed")
                .await;
            return Err("authentication failed".to_string());
        }

        // Send parameter status messages
        let params = ServerParameters::new("nucleus", "binary_client", "nucleus-binary");
        let param_bytes = handshake.send_parameters(params)?;
        self.stream
            .write_all(&param_bytes)
            .await
            .map_err(|e| e.to_string())?;

        // Send ready for query
        let ready_bytes = handshake.send_ready(ready_status::IDLE)?;
        self.stream
            .write_all(&ready_bytes)
            .await
            .map_err(|e| e.to_string())?;

        Ok(())
    }

    /// Main message dispatch loop.
    async fn message_loop(&mut self) -> std::io::Result<()> {
        loop {
            let frame = match self.read_frame().await? {
                Some(f) => f,
                None => break, // connection closed
            };

            match frame.message_type {
                message_types::QUERY => {
                    self.handle_query(&frame.payload).await?;
                }
                message_types::PREPARED_STMT => {
                    self.handle_prepare(&frame.payload).await?;
                }
                message_types::BIND => {
                    self.handle_bind(&frame.payload).await?;
                }
                message_types::EXECUTE => {
                    self.handle_execute(&frame.payload).await?;
                }
                message_types::BEGIN_TXN => {
                    self.handle_begin_txn(&frame.payload).await?;
                }
                message_types::COMMIT_TXN => {
                    self.handle_commit_txn().await?;
                }
                message_types::ROLLBACK_TXN => {
                    self.handle_rollback_txn().await?;
                }
                other => {
                    self.send_error(
                        error_codes::PROTOCOL_ERROR,
                        &format!("unexpected message type: {other}"),
                    )
                    .await;
                    self.send_ready().await?;
                }
            }
        }
        Ok(())
    }

    /// Handle a simple query message.
    async fn handle_query(&mut self, payload: &[u8]) -> std::io::Result<()> {
        let (_flags, _query_id, sql) = match Decoder::parse_query(payload) {
            Ok(parsed) => parsed,
            Err(e) => {
                self.send_error(error_codes::PROTOCOL_ERROR, &e.to_string())
                    .await;
                self.send_ready().await?;
                return Ok(());
            }
        };

        self.execute_sql(sql).await
    }

    /// Handle a prepared statement creation.
    async fn handle_prepare(&mut self, payload: &[u8]) -> std::io::Result<()> {
        let (stmt_id, sql) = match Decoder::parse_prepared_stmt(payload) {
            Ok(parsed) => parsed,
            Err(e) => {
                self.send_error(error_codes::PROTOCOL_ERROR, &e.to_string())
                    .await;
                self.send_ready().await?;
                return Ok(());
            }
        };

        match self.query_handler.prepare_statement(stmt_id, sql) {
            Ok(prepared) => {
                self.encoder.reset();
                self.encoder.encode_command_complete(
                    0,
                    &format!("PREPARE {}", prepared.param_count),
                );
                let buf = self.encoder.buffer().to_vec();
                self.stream.write_all(&buf).await?;
                self.send_ready().await?;
            }
            Err(e) => {
                self.send_error(error_codes::SYNTAX_ERROR, &e.to_string())
                    .await;
                self.send_ready().await?;
            }
        }
        Ok(())
    }

    /// Handle a bind message (parameters for prepared statement).
    async fn handle_bind(&mut self, payload: &[u8]) -> std::io::Result<()> {
        let (stmt_id, param_count, params_bytes) = match Decoder::parse_bind(payload) {
            Ok(parsed) => parsed,
            Err(e) => {
                self.send_error(error_codes::PROTOCOL_ERROR, &e.to_string())
                    .await;
                self.send_ready().await?;
                return Ok(());
            }
        };

        // Decode parameter values from wire format
        let params = match decode_params(param_count, params_bytes) {
            Ok(p) => p,
            Err(e) => {
                self.send_error(error_codes::PROTOCOL_ERROR, &e).await;
                self.send_ready().await?;
                return Ok(());
            }
        };

        // Bind parameters and execute
        match self.query_handler.bind_parameters(stmt_id, params) {
            Ok(substituted_sql) => {
                self.execute_sql(&substituted_sql).await?;
            }
            Err(e) => {
                self.send_error(error_codes::RUNTIME_ERROR, &e.to_string())
                    .await;
                self.send_ready().await?;
            }
        }
        Ok(())
    }

    /// Handle an execute message (run a prepared statement with bound params).
    async fn handle_execute(&mut self, payload: &[u8]) -> std::io::Result<()> {
        let (stmt_id, _flags) = match Decoder::parse_execute(payload) {
            Ok(parsed) => parsed,
            Err(e) => {
                self.send_error(error_codes::PROTOCOL_ERROR, &e.to_string())
                    .await;
                self.send_ready().await?;
                return Ok(());
            }
        };

        // Get the prepared statement SQL and execute it directly
        match self.query_handler.get_prepared(stmt_id) {
            Ok(prepared) => {
                self.execute_sql(&prepared.sql).await?;
            }
            Err(e) => {
                self.send_error(error_codes::RUNTIME_ERROR, &e.to_string())
                    .await;
                self.send_ready().await?;
            }
        }
        Ok(())
    }

    /// Handle BEGIN transaction.
    async fn handle_begin_txn(&mut self, payload: &[u8]) -> std::io::Result<()> {
        let _isolation_level = match Decoder::parse_begin_txn(payload) {
            Ok(level) => level,
            Err(e) => {
                self.send_error(error_codes::PROTOCOL_ERROR, &e.to_string())
                    .await;
                self.send_ready().await?;
                return Ok(());
            }
        };

        match self
            .executor
            .execute_with_session(self.session_id, "BEGIN")
            .await
        {
            Ok(_) => {
                self.in_transaction = true;
                self.txn_error = false;
                self.encoder.reset();
                self.encoder.encode_command_complete(0, "BEGIN");
                let buf = self.encoder.buffer().to_vec();
                self.stream.write_all(&buf).await?;
                self.send_ready().await?;
            }
            Err(e) => {
                self.send_error(error_codes::TRANSACTION_ERROR, &e.to_string())
                    .await;
                self.send_ready().await?;
            }
        }
        Ok(())
    }

    /// Handle COMMIT transaction.
    async fn handle_commit_txn(&mut self) -> std::io::Result<()> {
        match self
            .executor
            .execute_with_session(self.session_id, "COMMIT")
            .await
        {
            Ok(_) => {
                self.in_transaction = false;
                self.txn_error = false;
                self.encoder.reset();
                self.encoder.encode_command_complete(0, "COMMIT");
                let buf = self.encoder.buffer().to_vec();
                self.stream.write_all(&buf).await?;
                self.send_ready().await?;
            }
            Err(e) => {
                self.txn_error = true;
                self.send_error(error_codes::TRANSACTION_ERROR, &e.to_string())
                    .await;
                self.send_ready().await?;
            }
        }
        Ok(())
    }

    /// Handle ROLLBACK transaction.
    async fn handle_rollback_txn(&mut self) -> std::io::Result<()> {
        match self
            .executor
            .execute_with_session(self.session_id, "ROLLBACK")
            .await
        {
            Ok(_) => {
                self.in_transaction = false;
                self.txn_error = false;
                self.encoder.reset();
                self.encoder.encode_command_complete(0, "ROLLBACK");
                let buf = self.encoder.buffer().to_vec();
                self.stream.write_all(&buf).await?;
                self.send_ready().await?;
            }
            Err(e) => {
                self.send_error(error_codes::TRANSACTION_ERROR, &e.to_string())
                    .await;
                self.send_ready().await?;
            }
        }
        Ok(())
    }

    /// Execute SQL against the executor and send results back.
    async fn execute_sql(&mut self, sql: &str) -> std::io::Result<()> {
        match self
            .executor
            .execute_with_session(self.session_id, sql)
            .await
        {
            Ok(results) => {
                for result in results {
                    self.send_exec_result(result).await?;
                }
                self.send_ready().await?;
            }
            Err(e) => {
                if self.in_transaction {
                    self.txn_error = true;
                }
                self.send_error(error_codes::RUNTIME_ERROR, &e.to_string())
                    .await;
                self.send_ready().await?;
            }
        }
        Ok(())
    }

    /// Send an ExecResult as binary protocol frames.
    async fn send_exec_result(&mut self, result: ExecResult) -> std::io::Result<()> {
        match result {
            ExecResult::Select { columns, rows } => {
                // Send column metadata
                for (i, (name, dtype)) in columns.iter().enumerate() {
                    let type_code = datatype_to_type_code(dtype);
                    self.encoder.reset();
                    self.encoder
                        .encode_column_metadata(i as u16, name, type_code);
                    let buf = self.encoder.buffer().to_vec();
                    self.stream.write_all(&buf).await?;
                }

                // Send data rows
                let row_count = rows.len() as u32;
                for row in &rows {
                    let row_bytes = self.result_encoder.encode_row(row);
                    self.encoder.reset();
                    self.encoder
                        .encode_data_row(row.len() as u16, &row_bytes[2..]); // skip column_count prefix
                    let buf = self.encoder.buffer().to_vec();
                    self.stream.write_all(&buf).await?;
                }

                // Send result end
                self.encoder.reset();
                self.encoder.encode_result_end(row_count);
                let buf = self.encoder.buffer().to_vec();
                self.stream.write_all(&buf).await?;
            }
            ExecResult::Command {
                tag,
                rows_affected,
            } => {
                self.encoder.reset();
                self.encoder
                    .encode_command_complete(rows_affected as u32, &tag);
                let buf = self.encoder.buffer().to_vec();
                self.stream.write_all(&buf).await?;
            }
            ExecResult::CopyOut { data, row_count } => {
                self.encoder.reset();
                self.encoder
                    .encode_command_complete(row_count as u32, &format!("COPY {row_count}"));
                let buf = self.encoder.buffer().to_vec();
                self.stream.write_all(&buf).await?;
                // Send the copy data as a data row frame
                if !data.is_empty() {
                    self.encoder.reset();
                    self.encoder
                        .encode_data_row(1, data.as_bytes());
                    let buf = self.encoder.buffer().to_vec();
                    self.stream.write_all(&buf).await?;
                }
            }
        }
        Ok(())
    }

    /// Send an error response frame.
    async fn send_error(&mut self, code: u16, message: &str) {
        self.encoder.reset();
        self.encoder.encode_error(code, message);
        let buf = self.encoder.buffer().to_vec();
        let _ = self.stream.write_all(&buf).await;
    }

    /// Send a ready-for-query frame.
    async fn send_ready(&mut self) -> std::io::Result<()> {
        let status = if self.txn_error {
            ready_status::ERROR
        } else if self.in_transaction {
            ready_status::IN_TXN
        } else {
            ready_status::IDLE
        };
        self.encoder.reset();
        self.encoder.encode_ready(status);
        let buf = self.encoder.buffer().to_vec();
        self.stream.write_all(&buf).await
    }

    /// Read a single frame from the TCP stream, buffering incomplete data.
    async fn read_frame(&mut self) -> std::io::Result<Option<DecodedFrame>> {
        loop {
            // Try to parse a frame from buffered data first
            match self.decoder.parse_frame() {
                Ok(Some(frame)) => return Ok(Some(frame)),
                Ok(None) => {
                    // Need more data
                    let mut buf = [0u8; 8192];
                    let n = self.stream.read(&mut buf).await?;
                    if n == 0 {
                        return Ok(None); // connection closed
                    }
                    self.decoder.feed(&buf[..n]);
                }
                Err(e) => {
                    tracing::debug!("Binary protocol decode error: {e}");
                    return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()));
                }
            }
        }
    }
}

/// Map nucleus DataType to binary protocol type code.
fn datatype_to_type_code(dtype: &DataType) -> u8 {
    match dtype {
        DataType::Bool => type_codes::BOOL,
        DataType::Int32 => type_codes::INT32,
        DataType::Int64 => type_codes::INT64,
        DataType::Float64 => type_codes::FLOAT64,
        DataType::Text => type_codes::STRING,
        DataType::Jsonb => type_codes::JSON,
        DataType::Bytea => type_codes::BYTES,
        DataType::Vector(_) => type_codes::VECTOR,
        DataType::Uuid => type_codes::UUID,
        DataType::Timestamp | DataType::TimestampTz => type_codes::TIMESTAMP,
        DataType::Numeric => type_codes::DECIMAL,
        DataType::Interval => type_codes::INTERVAL,
        DataType::Array(_) => type_codes::ARRAY,
        _ => type_codes::STRING, // fallback
    }
}

/// Decode parameter values from wire bytes.
///
/// Wire format per param: [type:1][len:4][data:len]
fn decode_params(param_count: u16, data: &[u8]) -> Result<Vec<crate::types::Value>, String> {
    use crate::types::Value;
    let mut params = Vec::with_capacity(param_count as usize);
    let mut offset = 0;

    for _ in 0..param_count {
        if offset >= data.len() {
            return Err("parameter data truncated".to_string());
        }

        let type_code = data[offset];
        offset += 1;

        if offset + 4 > data.len() {
            return Err("parameter length truncated".to_string());
        }
        let len = u32::from_be_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]) as usize;
        offset += 4;

        if offset + len > data.len() {
            return Err("parameter value truncated".to_string());
        }
        let value_bytes = &data[offset..offset + len];
        offset += len;

        let value = match type_code {
            type_codes::NULL => Value::Null,
            type_codes::BOOL => {
                if len < 1 {
                    return Err("bool value too short".to_string());
                }
                Value::Bool(value_bytes[0] != 0)
            }
            type_codes::INT32 => {
                if len < 4 {
                    return Err("int32 value too short".to_string());
                }
                Value::Int32(i32::from_be_bytes([
                    value_bytes[0],
                    value_bytes[1],
                    value_bytes[2],
                    value_bytes[3],
                ]))
            }
            type_codes::INT64 => {
                if len < 8 {
                    return Err("int64 value too short".to_string());
                }
                Value::Int64(i64::from_be_bytes([
                    value_bytes[0],
                    value_bytes[1],
                    value_bytes[2],
                    value_bytes[3],
                    value_bytes[4],
                    value_bytes[5],
                    value_bytes[6],
                    value_bytes[7],
                ]))
            }
            type_codes::FLOAT64 => {
                if len < 8 {
                    return Err("float64 value too short".to_string());
                }
                Value::Float64(f64::from_be_bytes([
                    value_bytes[0],
                    value_bytes[1],
                    value_bytes[2],
                    value_bytes[3],
                    value_bytes[4],
                    value_bytes[5],
                    value_bytes[6],
                    value_bytes[7],
                ]))
            }
            type_codes::STRING => {
                let s = std::str::from_utf8(value_bytes)
                    .map_err(|_| "invalid UTF-8 in string param".to_string())?;
                Value::Text(s.to_string())
            }
            _ => {
                // Treat unknown types as text
                let s = std::str::from_utf8(value_bytes)
                    .map_err(|_| "invalid UTF-8 in fallback param".to_string())?;
                Value::Text(s.to_string())
            }
        };

        params.push(value);
    }

    Ok(params)
}
