//! Week 1: Test Server Infrastructure
//!
//! Provides TestServer (spawns binary protocol listener) and TestClient
//! (connects to binary protocol, sends queries, decodes rows).
//!
//! The test harness allows parallel execution of test cases without
//! blocking the main development phases (Phase 1, 2, 4).

use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::io;

use crate::catalog::Catalog;
use crate::executor::Executor;
use crate::storage::MemoryEngine;
use crate::types::{Row, Value, DataType};

// ============================================================================
// Test Server Infrastructure
// ============================================================================

/// Spawns a binary protocol server listening on a test port.
/// Returns a handle to connect test clients.
#[allow(dead_code)]
pub async fn spawn_binary_server(port: u16) -> io::Result<TestServer> {
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await?;

    // Create executor with in-memory storage
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn crate::storage::StorageEngine> = Arc::new(MemoryEngine::new());
    let executor = Arc::new(Executor::new(catalog, storage));

    Ok(TestServer {
        listener,
        executor,
        port,
    })
}

/// Test server handle
pub struct TestServer {
    pub listener: TcpListener,
    pub executor: Arc<Executor>,
    pub port: u16,
}

impl TestServer {
    /// Accept one incoming connection and handle it in background.
    /// Returns immediately; handler runs in background task.
    #[allow(dead_code)]
    pub async fn accept_one(&self) {
        let (socket, _) = self.listener.accept().await.ok().unwrap();
        let executor = self.executor.clone();

        tokio::spawn(async move {
            let mut handler = ConnectionHandler::new(socket, executor);
            let _ = handler.run().await;
        });
    }

    /// Wait for server to accept and handle one client (blocking).
    /// Used in tests to ensure connection is ready before assertions.
    #[allow(dead_code)]
    pub async fn accept_and_handle(&self) {
        self.accept_one().await;
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }
}

// ============================================================================
// Connection Handler (processes binary protocol from clients)
// ============================================================================

struct ConnectionHandler {
    socket: TcpStream,
    executor: Arc<Executor>,
    buf: Vec<u8>,
}

impl ConnectionHandler {
    fn new(socket: TcpStream, executor: Arc<Executor>) -> Self {
        Self {
            socket,
            executor,
            buf: vec![0u8; 8192],
        }
    }

    async fn run(&mut self) -> io::Result<()> {
        loop {
            let n = self.socket.read(&mut self.buf).await?;
            if n == 0 {
                break; // connection closed
            }

            // TODO: Parse binary protocol message from buf[0..n]
            // For now, this is a placeholder. In Phase 1, we implement the binary codec.

            // Send placeholder response
            self.socket.write_all(b"OK\n").await?;
        }
        Ok(())
    }
}

// ============================================================================
// Test Client (connects to binary protocol server)
// ============================================================================

/// Test client that connects via binary protocol.
/// Sends queries and decodes results.
pub struct TestClient {
    socket: TcpStream,
    // Placeholder for BinaryEncoder and BinaryDecoder
    // TODO: These will be implemented in Phase 1
}

impl TestClient {
    /// Connect to a binary protocol server at the given address.
    #[allow(dead_code)]
    pub async fn connect(addr: &str) -> io::Result<Self> {
        let socket = TcpStream::connect(addr).await?;
        Ok(TestClient { socket })
    }

    /// Send a SQL query and receive decoded rows.
    #[allow(dead_code)]
    pub async fn query(&mut self, sql: &str) -> io::Result<Vec<Row>> {
        // TODO: Phase 1 implementation
        // 1. Encode query as binary protocol message
        // 2. Send to socket
        // 3. Read response
        // 4. Decode rows
        // 5. Return rows

        // For now, return empty vector (placeholder)
        Ok(Vec::new())
    }

    /// Send a prepared statement with bind parameters.
    #[allow(dead_code)]
    pub async fn execute_prepared(
        &mut self,
        stmt_id: u32,
        params: Vec<Value>,
    ) -> io::Result<Vec<Row>> {
        // TODO: Phase 1 implementation
        Ok(Vec::new())
    }
}

// ============================================================================
// Helper: Protocol Message Encoder/Decoder (stubs)
// ============================================================================

#[allow(dead_code)]
struct BinaryEncoder;

#[allow(dead_code)]
impl BinaryEncoder {
    fn encode_query(sql: &str) -> Vec<u8> {
        // TODO: Phase 1 — encode query as binary protocol message
        // Expected format (custom):
        // - 1 byte: message type (e.g., 'Q' for query)
        // - 4 bytes: message length (big-endian)
        // - N bytes: SQL text
        vec![]
    }

    fn encode_prepared_execute(stmt_id: u32, params: &[Value]) -> Vec<u8> {
        // TODO: Phase 1
        vec![]
    }
}

#[allow(dead_code)]
struct BinaryDecoder;

#[allow(dead_code)]
impl BinaryDecoder {
    fn decode_row(data: &[u8]) -> io::Result<Row> {
        // TODO: Phase 1 — decode binary row format
        // Expected format:
        // - 2 bytes: column count
        // - For each column:
        //   - 1 byte: type indicator
        //   - 4 bytes: value length
        //   - N bytes: encoded value
        Ok(Vec::new())
    }

    fn decode_rows(data: &[u8]) -> io::Result<Vec<Row>> {
        // TODO: Phase 1
        Ok(Vec::new())
    }
}

// ============================================================================
// Tests: Server Lifecycle
// ============================================================================

#[tokio::test]
async fn test_server_startup() {
    let server = spawn_binary_server(9001).await.expect("failed to spawn server");
    assert_eq!(server.port, 9001);
}

#[tokio::test]
async fn test_client_connect() {
    let server = spawn_binary_server(9002).await.expect("failed to spawn server");

    // Spawn server handler in background
    tokio::spawn(async move {
        server.accept_and_handle().await;
    });

    // Wait for server to start accepting
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let result = TestClient::connect("127.0.0.1:9002").await;
    assert!(result.is_ok(), "client should connect successfully");
}
