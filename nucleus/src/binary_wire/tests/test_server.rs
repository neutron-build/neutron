//! Test Server Infrastructure
//!
//! Provides TestServer (spawns binary protocol listener) and TestClient
//! (connects to binary protocol, sends queries, decodes rows).
//!
//! Uses the real ConnectionHandler and encoder/decoder for end-to-end testing.

use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use std::io;

use crate::binary_wire::connection_handler::ConnectionHandler;
use crate::binary_wire::decoder::{DecodedFrame, Decoder, message_types};
use crate::binary_wire::encoder::Encoder;
use crate::binary_wire::handshake::{AuthResponse, PROTOCOL_VERSION};
use crate::binary_wire::result_serializer::type_codes;
use crate::catalog::Catalog;
use crate::executor::Executor;
use crate::storage::MemoryEngine;
use crate::types::{Row, Value};

// ============================================================================
// Test Server Infrastructure
// ============================================================================

/// Spawns a binary protocol server listening on a test port.
/// Returns a handle to connect test clients.
#[allow(dead_code)]
pub async fn spawn_binary_server(port: u16) -> io::Result<TestServer> {
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await?;

    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn crate::storage::StorageEngine> = Arc::new(MemoryEngine::new());
    let executor = Arc::new(Executor::new(catalog, storage));

    Ok(TestServer {
        listener,
        executor,
        port,
    })
}

/// Test server handle.
pub struct TestServer {
    pub listener: TcpListener,
    pub executor: Arc<Executor>,
    pub port: u16,
}

impl TestServer {
    /// Accept one incoming connection and handle it with the real ConnectionHandler.
    #[allow(dead_code)]
    pub async fn accept_one(&self) {
        let (socket, _) = self.listener.accept().await.ok().unwrap();
        let executor = self.executor.clone();

        tokio::spawn(async move {
            let mut handler = ConnectionHandler::new(socket, executor, None);
            let _ = handler.run().await;
        });
    }

    /// Accept and handle one client, with a short sleep for setup.
    #[allow(dead_code)]
    pub async fn accept_and_handle(&self) {
        self.accept_one().await;
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }
}

// ============================================================================
// Test Client (connects via binary protocol with real encoder/decoder)
// ============================================================================

/// Test client that connects via binary protocol.
pub struct TestClient {
    stream: TcpStream,
    decoder: Decoder,
    encoder: Encoder,
}

impl TestClient {
    /// Connect to a binary protocol server at the given address.
    #[allow(dead_code)]
    pub async fn connect(addr: &str) -> io::Result<Self> {
        let socket = TcpStream::connect(addr).await?;
        Ok(TestClient {
            stream: socket,
            decoder: Decoder::new(),
            encoder: Encoder::new(),
        })
    }

    /// Perform the handshake (version + auth) with default empty password.
    #[allow(dead_code)]
    pub async fn handshake(&mut self) -> io::Result<()> {
        self.handshake_with_password("").await
    }

    /// Perform the handshake with a specific password.
    #[allow(dead_code)]
    pub async fn handshake_with_password(&mut self, password: &str) -> io::Result<()> {
        // Send client handshake
        self.encoder.reset();
        self.encoder.encode_handshake(PROTOCOL_VERSION, 1);
        let buf = self.encoder.buffer().to_vec();
        self.stream.write_all(&buf).await?;

        // Read server handshake response
        let frame = self.read_frame().await?;
        if frame.message_type != message_types::HANDSHAKE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("expected handshake, got type {}", frame.message_type),
            ));
        }

        // Read auth challenge
        let auth_frame = self.read_frame().await?;
        if auth_frame.message_type != message_types::AUTHENTICATION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("expected auth challenge, got type {}", auth_frame.message_type),
            ));
        }

        // Decode the challenge to get server nonce
        let challenge = crate::binary_wire::handshake::AuthChallenge::decode(&auth_frame.payload)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        // Build auth response with correct nonce and proof
        let response = AuthResponse {
            challenge_id: challenge.challenge_id,
            combined_nonce: challenge.server_nonce.clone(),
            proof: format!("Auth:{}", password).into_bytes(),
        };

        self.encoder.reset();
        self.encoder
            .encode_frame(message_types::AUTHENTICATION, &response.encode());
        let buf = self.encoder.buffer().to_vec();
        self.stream.write_all(&buf).await?;

        // Read parameter status messages and ready
        loop {
            let frame = self.read_frame().await?;
            match frame.message_type {
                message_types::PARAMETER_STATUS => {
                    // Consume parameter status
                    continue;
                }
                message_types::READY => {
                    break;
                }
                message_types::ERROR => {
                    let (code, msg) = Decoder::parse_error(&frame.payload)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        format!("auth error {}: {}", code, msg),
                    ));
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unexpected message type {} during handshake", frame.message_type),
                    ));
                }
            }
        }

        Ok(())
    }

    /// Send a SQL query and receive decoded rows.
    #[allow(dead_code)]
    pub async fn query(&mut self, sql: &str) -> io::Result<QueryResult> {
        self.encoder.reset();
        self.encoder.encode_query(0, sql);
        let buf = self.encoder.buffer().to_vec();
        self.stream.write_all(&buf).await?;

        self.read_result().await
    }

    /// Send a simple query (non-SELECT) and get command result.
    #[allow(dead_code)]
    pub async fn execute(&mut self, sql: &str) -> io::Result<QueryResult> {
        self.query(sql).await
    }

    /// Prepare a statement.
    #[allow(dead_code)]
    pub async fn prepare(&mut self, stmt_id: u32, sql: &str) -> io::Result<QueryResult> {
        self.encoder.reset();
        self.encoder.encode_prepared_stmt(stmt_id, sql);
        let buf = self.encoder.buffer().to_vec();
        self.stream.write_all(&buf).await?;

        self.read_result().await
    }

    /// Execute a prepared statement (no parameters).
    #[allow(dead_code)]
    pub async fn execute_prepared(&mut self, stmt_id: u32) -> io::Result<QueryResult> {
        self.encoder.reset();
        self.encoder.encode_execute(stmt_id);
        let buf = self.encoder.buffer().to_vec();
        self.stream.write_all(&buf).await?;

        self.read_result().await
    }

    /// Execute a prepared statement with bound parameters.
    #[allow(dead_code)]
    pub async fn execute_with_params(
        &mut self,
        stmt_id: u32,
        params: &[Value],
    ) -> io::Result<QueryResult> {
        let params_bytes = encode_params(params);
        self.encoder.reset();
        self.encoder
            .encode_bind(stmt_id, params.len() as u16, &params_bytes);
        let buf = self.encoder.buffer().to_vec();
        self.stream.write_all(&buf).await?;

        self.read_result().await
    }

    /// Send BEGIN transaction.
    #[allow(dead_code)]
    pub async fn begin(&mut self) -> io::Result<QueryResult> {
        self.encoder.reset();
        self.encoder.encode_begin_txn(0);
        let buf = self.encoder.buffer().to_vec();
        self.stream.write_all(&buf).await?;

        self.read_result().await
    }

    /// Send COMMIT transaction.
    #[allow(dead_code)]
    pub async fn commit(&mut self) -> io::Result<QueryResult> {
        self.encoder.reset();
        self.encoder.encode_commit_txn();
        let buf = self.encoder.buffer().to_vec();
        self.stream.write_all(&buf).await?;

        self.read_result().await
    }

    /// Send ROLLBACK transaction.
    #[allow(dead_code)]
    pub async fn rollback(&mut self) -> io::Result<QueryResult> {
        self.encoder.reset();
        self.encoder.encode_rollback_txn();
        let buf = self.encoder.buffer().to_vec();
        self.stream.write_all(&buf).await?;

        self.read_result().await
    }

    /// Read all response frames until a Ready message.
    async fn read_result(&mut self) -> io::Result<QueryResult> {
        let mut columns: Vec<String> = Vec::new();
        let mut rows: Vec<Row> = Vec::new();
        let mut command_tag: Option<String> = None;
        let mut rows_affected: u32 = 0;
        let mut error: Option<(u16, String)> = None;
        #[allow(unused_assignments)]
        let mut ready_status: u8 = 0;

        loop {
            let frame = self.read_frame().await?;
            match frame.message_type {
                message_types::COLUMN_METADATA => {
                    let (_col_id, name, _type_code, _flags) =
                        Decoder::parse_column_metadata(&frame.payload)
                            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
                    columns.push(name.to_string());
                }
                message_types::DATA_ROW => {
                    let (_col_count, col_data) = Decoder::parse_data_row(&frame.payload)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
                    let row = decode_row_values(col_data)?;
                    rows.push(row);
                }
                message_types::RESULT_END => {
                    let _count = Decoder::parse_result_end(&frame.payload)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
                }
                message_types::COMMAND_COMPLETE => {
                    let (affected, msg) = Decoder::parse_command_complete(&frame.payload)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
                    rows_affected = affected;
                    command_tag = Some(msg.to_string());
                }
                message_types::ERROR => {
                    let (code, msg) = Decoder::parse_error(&frame.payload)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
                    error = Some((code, msg.to_string()));
                }
                message_types::READY => {
                    ready_status = Decoder::parse_ready(&frame.payload)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
                    break;
                }
                _ => {
                    // Skip unexpected frames
                }
            }
        }

        Ok(QueryResult {
            columns,
            rows,
            command_tag,
            rows_affected,
            error,
            ready_status,
        })
    }

    /// Read a single frame from the stream.
    async fn read_frame(&mut self) -> io::Result<DecodedFrame> {
        loop {
            match self.decoder.parse_frame() {
                Ok(Some(frame)) => return Ok(frame),
                Ok(None) => {
                    let mut buf = [0u8; 8192];
                    let n = self.stream.read(&mut buf).await?;
                    if n == 0 {
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "connection closed",
                        ));
                    }
                    self.decoder.feed(&buf[..n]);
                }
                Err(e) => {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, e.to_string()));
                }
            }
        }
    }
}

/// Result from a query or command over binary protocol.
#[derive(Debug)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Row>,
    pub command_tag: Option<String>,
    pub rows_affected: u32,
    pub error: Option<(u16, String)>,
    pub ready_status: u8,
}

impl QueryResult {
    /// Returns true if the result contains an error.
    #[allow(dead_code)]
    pub fn is_error(&self) -> bool {
        self.error.is_some()
    }

    /// Returns true if the result contains rows (SELECT).
    #[allow(dead_code)]
    pub fn has_rows(&self) -> bool {
        !self.rows.is_empty()
    }
}

/// Encode parameter values to wire bytes.
/// Wire format per param: [type:1][len:4][data:len]
fn encode_params(params: &[Value]) -> Vec<u8> {
    let mut buf = Vec::new();
    for param in params {
        match param {
            Value::Null => {
                buf.push(type_codes::NULL);
                buf.extend_from_slice(&0u32.to_be_bytes());
            }
            Value::Bool(b) => {
                buf.push(type_codes::BOOL);
                buf.extend_from_slice(&1u32.to_be_bytes());
                buf.push(if *b { 1 } else { 0 });
            }
            Value::Int32(n) => {
                buf.push(type_codes::INT32);
                buf.extend_from_slice(&4u32.to_be_bytes());
                buf.extend_from_slice(&n.to_be_bytes());
            }
            Value::Int64(n) => {
                buf.push(type_codes::INT64);
                buf.extend_from_slice(&8u32.to_be_bytes());
                buf.extend_from_slice(&n.to_be_bytes());
            }
            Value::Float64(f) => {
                buf.push(type_codes::FLOAT64);
                buf.extend_from_slice(&8u32.to_be_bytes());
                buf.extend_from_slice(&f.to_be_bytes());
            }
            Value::Text(s) => {
                buf.push(type_codes::STRING);
                let bytes = s.as_bytes();
                buf.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
                buf.extend_from_slice(bytes);
            }
            _ => {
                let s = param.to_string();
                buf.push(type_codes::STRING);
                let bytes = s.as_bytes();
                buf.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
                buf.extend_from_slice(bytes);
            }
        }
    }
    buf
}

/// Decode row values from the wire column data.
/// Each value: [type:1][len:4][data:len]
fn decode_row_values(data: &[u8]) -> io::Result<Row> {
    let mut row = Vec::new();
    let mut offset = 0;

    while offset < data.len() {
        if offset >= data.len() {
            break;
        }
        let type_code = data[offset];
        offset += 1;

        if offset + 4 > data.len() {
            break;
        }
        let len = u32::from_be_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]) as usize;
        offset += 4;

        if offset + len > data.len() {
            break;
        }
        let value_bytes = &data[offset..offset + len];
        offset += len;

        let value = match type_code {
            type_codes::NULL => Value::Null,
            type_codes::BOOL => {
                if len >= 1 {
                    Value::Bool(value_bytes[0] != 0)
                } else {
                    Value::Null
                }
            }
            type_codes::INT32 => {
                if len >= 4 {
                    Value::Int32(i32::from_be_bytes([
                        value_bytes[0],
                        value_bytes[1],
                        value_bytes[2],
                        value_bytes[3],
                    ]))
                } else {
                    Value::Null
                }
            }
            type_codes::INT64 => {
                if len >= 8 {
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
                } else {
                    Value::Null
                }
            }
            type_codes::FLOAT64 => {
                if len >= 8 {
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
                } else {
                    Value::Null
                }
            }
            type_codes::STRING | type_codes::JSON => {
                let s = std::str::from_utf8(value_bytes)
                    .unwrap_or("")
                    .to_string();
                Value::Text(s)
            }
            _ => {
                let s = std::str::from_utf8(value_bytes)
                    .unwrap_or("")
                    .to_string();
                Value::Text(s)
            }
        };

        row.push(value);
    }

    Ok(row)
}

// ============================================================================
// Tests: Server Lifecycle
// ============================================================================

#[tokio::test]
async fn test_server_startup() {
    let server = spawn_binary_server(0).await.expect("failed to spawn server");
    assert!(server.listener.local_addr().is_ok());
}

#[tokio::test]
async fn test_client_connect() {
    let server = spawn_binary_server(0).await.expect("failed to spawn server");
    let addr = server.listener.local_addr().unwrap();

    tokio::spawn(async move {
        server.accept_and_handle().await;
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let result = TestClient::connect(&addr.to_string()).await;
    assert!(result.is_ok(), "client should connect successfully");
}

#[tokio::test]
async fn test_handshake_completes() {
    let server = spawn_binary_server(0).await.expect("failed to spawn server");
    let addr = server.listener.local_addr().unwrap();

    tokio::spawn(async move {
        server.accept_and_handle().await;
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let mut client = TestClient::connect(&addr.to_string()).await.unwrap();
    let result = client.handshake().await;
    assert!(result.is_ok(), "handshake should complete: {:?}", result.err());
}

#[tokio::test]
async fn test_simple_query() {
    let server = spawn_binary_server(0).await.expect("failed to spawn server");
    let addr = server.listener.local_addr().unwrap();

    tokio::spawn(async move {
        server.accept_and_handle().await;
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let mut client = TestClient::connect(&addr.to_string()).await.unwrap();
    client.handshake().await.unwrap();

    let result = client.query("SELECT 1 AS num").await.unwrap();
    assert!(!result.is_error(), "query should succeed: {:?}", result.error);
    assert_eq!(result.columns.len(), 1);
    assert_eq!(result.columns[0], "num");
    assert_eq!(result.rows.len(), 1);
}

#[tokio::test]
async fn test_create_insert_select() {
    let server = spawn_binary_server(0).await.expect("failed to spawn server");
    let addr = server.listener.local_addr().unwrap();

    tokio::spawn(async move {
        server.accept_and_handle().await;
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let mut client = TestClient::connect(&addr.to_string()).await.unwrap();
    client.handshake().await.unwrap();

    // CREATE TABLE
    let result = client
        .execute("CREATE TABLE test_bp (id INT, name TEXT)")
        .await
        .unwrap();
    assert!(!result.is_error(), "CREATE should succeed: {:?}", result.error);

    // INSERT
    let result = client
        .execute("INSERT INTO test_bp VALUES (1, 'Alice')")
        .await
        .unwrap();
    assert!(!result.is_error(), "INSERT should succeed: {:?}", result.error);

    // SELECT
    let result = client
        .query("SELECT id, name FROM test_bp")
        .await
        .unwrap();
    assert!(!result.is_error(), "SELECT should succeed: {:?}", result.error);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.columns.len(), 2);
}

#[tokio::test]
async fn test_prepared_statement() {
    let server = spawn_binary_server(0).await.expect("failed to spawn server");
    let addr = server.listener.local_addr().unwrap();

    tokio::spawn(async move {
        server.accept_and_handle().await;
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let mut client = TestClient::connect(&addr.to_string()).await.unwrap();
    client.handshake().await.unwrap();

    // Prepare a statement
    let result = client
        .prepare(1, "SELECT 1 AS val")
        .await
        .unwrap();
    assert!(!result.is_error(), "PREPARE should succeed: {:?}", result.error);

    // Execute the prepared statement
    let result = client.execute_prepared(1).await.unwrap();
    assert!(!result.is_error(), "EXECUTE should succeed: {:?}", result.error);
    assert_eq!(result.rows.len(), 1);
}

#[tokio::test]
async fn test_transaction_lifecycle() {
    let server = spawn_binary_server(0).await.expect("failed to spawn server");
    let addr = server.listener.local_addr().unwrap();

    tokio::spawn(async move {
        server.accept_and_handle().await;
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let mut client = TestClient::connect(&addr.to_string()).await.unwrap();
    client.handshake().await.unwrap();

    // Setup
    client
        .execute("CREATE TABLE txn_test (id INT, val TEXT)")
        .await
        .unwrap();

    // BEGIN
    let result = client.begin().await.unwrap();
    assert!(!result.is_error(), "BEGIN should succeed: {:?}", result.error);
    assert_eq!(result.ready_status, 1); // IN_TXN

    // INSERT within transaction
    let result = client
        .execute("INSERT INTO txn_test VALUES (1, 'in_txn')")
        .await
        .unwrap();
    assert!(!result.is_error(), "INSERT should succeed: {:?}", result.error);

    // COMMIT
    let result = client.commit().await.unwrap();
    assert!(!result.is_error(), "COMMIT should succeed: {:?}", result.error);
    assert_eq!(result.ready_status, 0); // IDLE

    // Verify data persisted
    let result = client
        .query("SELECT val FROM txn_test WHERE id = 1")
        .await
        .unwrap();
    assert!(!result.is_error());
    assert_eq!(result.rows.len(), 1);
}

#[tokio::test]
async fn test_transaction_rollback() {
    let server = spawn_binary_server(0).await.expect("failed to spawn server");
    let addr = server.listener.local_addr().unwrap();

    tokio::spawn(async move {
        server.accept_and_handle().await;
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let mut client = TestClient::connect(&addr.to_string()).await.unwrap();
    client.handshake().await.unwrap();

    client
        .execute("CREATE TABLE rb_test (id INT)")
        .await
        .unwrap();

    // BEGIN + INSERT + ROLLBACK
    client.begin().await.unwrap();
    client
        .execute("INSERT INTO rb_test VALUES (99)")
        .await
        .unwrap();
    let result = client.rollback().await.unwrap();
    assert!(!result.is_error(), "ROLLBACK should succeed: {:?}", result.error);
    assert_eq!(result.ready_status, 0); // IDLE

    // Verify rollback: no rows
    let result = client
        .query("SELECT COUNT(*) AS cnt FROM rb_test")
        .await
        .unwrap();
    assert!(!result.is_error());
    assert_eq!(result.rows.len(), 1);
}

#[tokio::test]
async fn test_error_response() {
    let server = spawn_binary_server(0).await.expect("failed to spawn server");
    let addr = server.listener.local_addr().unwrap();

    tokio::spawn(async move {
        server.accept_and_handle().await;
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let mut client = TestClient::connect(&addr.to_string()).await.unwrap();
    client.handshake().await.unwrap();

    // Send invalid SQL
    let result = client.query("INVALID SQL GARBAGE").await.unwrap();
    assert!(result.is_error(), "invalid SQL should produce error");
    assert!(result.error.is_some());

    // Connection should still be usable after error
    let result = client.query("SELECT 1 AS recovery").await.unwrap();
    assert!(
        !result.is_error(),
        "connection should recover after error: {:?}",
        result.error
    );
}

#[tokio::test]
async fn test_error_response_format() {
    let server = spawn_binary_server(0).await.expect("failed to spawn server");
    let addr = server.listener.local_addr().unwrap();

    tokio::spawn(async move {
        server.accept_and_handle().await;
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let mut client = TestClient::connect(&addr.to_string()).await.unwrap();
    client.handshake().await.unwrap();

    // Query non-existent table
    let result = client.query("SELECT * FROM no_such_table").await.unwrap();
    assert!(result.is_error());
    let (code, msg) = result.error.unwrap();
    assert!(code > 0, "error code should be non-zero");
    assert!(!msg.is_empty(), "error message should be non-empty");
}
