//! Binary protocol TCP server.
//!
//! Listens for connections and spawns a [`ConnectionHandler`] per client.
//! Same pattern as the RESP server in `src/resp/server.rs`.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::net::TcpListener;

use crate::executor::Executor;
use super::connection_handler::ConnectionHandler;

/// Configuration for the binary protocol server.
pub struct BinaryServerConfig {
    /// Maximum concurrent connections (default 1024).
    pub max_connections: usize,
}

impl Default for BinaryServerConfig {
    fn default() -> Self {
        Self {
            max_connections: 1024,
        }
    }
}

/// Start the binary protocol server, accepting connections until `shutdown` is notified.
pub async fn start_binary_server(
    bind_addr: String,
    executor: Arc<Executor>,
    password: Option<String>,
    shutdown: Arc<tokio::sync::Notify>,
) -> std::io::Result<()> {
    start_binary_server_with_config(
        bind_addr,
        executor,
        password,
        shutdown,
        BinaryServerConfig::default(),
    )
    .await
}

/// Start the binary protocol server with full configuration.
pub async fn start_binary_server_with_config(
    bind_addr: String,
    executor: Arc<Executor>,
    password: Option<String>,
    shutdown: Arc<tokio::sync::Notify>,
    config: BinaryServerConfig,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(&bind_addr).await?;
    let active_connections = Arc::new(AtomicUsize::new(0));
    let max_connections = config.max_connections;

    tracing::info!("Binary protocol server listening on {}", bind_addr);

    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, addr) = result?;

                let current = active_connections.load(Ordering::Relaxed);
                if current >= max_connections {
                    tracing::warn!(
                        "Binary protocol connection limit reached ({max_connections}), rejecting {addr}"
                    );
                    drop(stream);
                    continue;
                }

                active_connections.fetch_add(1, Ordering::Relaxed);
                let conn_counter = Arc::clone(&active_connections);
                let exec = Arc::clone(&executor);
                let pw = password.clone();

                tokio::spawn(async move {
                    let mut handler = ConnectionHandler::new(stream, exec, pw);
                    if let Err(e) = handler.run().await {
                        tracing::debug!("Binary protocol connection error from {addr}: {e}");
                    }
                    conn_counter.fetch_sub(1, Ordering::Relaxed);
                });
            }
            _ = shutdown.notified() => {
                tracing::info!("Binary protocol server shutting down");
                break;
            }
        }
    }

    Ok(())
}
