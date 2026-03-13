//! RESP2 TCP server.
//!
//! Listens for Redis-compatible connections and dispatches commands to the
//! [`RespHandler`]. Each connection gets its own handler instance so
//! authentication state is per-connection.
//!
//! Supports optional TLS, connection limits, and idle timeouts.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::io::{AsyncRead, AsyncWrite, BufReader};
use tokio::net::TcpListener;

use crate::kv::KvStore;

/// Configuration for the RESP server.
pub struct RespServerConfig {
    /// Maximum concurrent connections (default 1024).
    pub max_connections: usize,
    /// Idle timeout in seconds — connections with no activity are closed (default 300).
    pub idle_timeout_secs: u64,
    /// Optional TLS config. When present, all connections are TLS-encrypted.
    pub tls_config: Option<Arc<pgwire::tokio::tokio_rustls::rustls::ServerConfig>>,
}

impl Default for RespServerConfig {
    fn default() -> Self {
        Self {
            max_connections: 1024,
            idle_timeout_secs: 300,
            tls_config: None,
        }
    }
}

/// Start the RESP2 server, accepting connections until `shutdown` is notified.
pub async fn start_resp_server(
    bind_addr: String,
    kv: Arc<KvStore>,
    password: Option<String>,
    shutdown: Arc<tokio::sync::Notify>,
) -> std::io::Result<()> {
    start_resp_server_with_config(bind_addr, kv, password, shutdown, RespServerConfig::default()).await
}

/// Start the RESP2 server with full configuration (TLS, connection limits, idle timeout).
pub async fn start_resp_server_with_config(
    bind_addr: String,
    kv: Arc<KvStore>,
    password: Option<String>,
    shutdown: Arc<tokio::sync::Notify>,
    config: RespServerConfig,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(&bind_addr).await?;
    let active_connections = Arc::new(AtomicUsize::new(0));
    let max_connections = config.max_connections;
    let idle_timeout = std::time::Duration::from_secs(config.idle_timeout_secs);

    let tls_acceptor = config.tls_config.map(|cfg| {
        pgwire::tokio::tokio_rustls::TlsAcceptor::from(cfg)
    });

    if tls_acceptor.is_some() {
        tracing::info!("RESP server listening on {} (TLS enabled)", bind_addr);
    } else {
        tracing::info!("RESP server listening on {}", bind_addr);
    }

    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, addr) = result?;

                // Check connection limit
                let current = active_connections.load(Ordering::Relaxed);
                if current >= max_connections {
                    tracing::warn!("RESP connection limit reached ({max_connections}), rejecting {addr}");
                    drop(stream);
                    continue;
                }

                active_connections.fetch_add(1, Ordering::Relaxed);
                let conn_counter = Arc::clone(&active_connections);
                let kv = Arc::clone(&kv);
                let pw = password.clone();
                let tls = tls_acceptor.clone();
                let timeout = idle_timeout;

                tokio::spawn(async move {
                    let result = if let Some(acceptor) = tls {
                        match acceptor.accept(stream).await {
                            Ok(tls_stream) => {
                                handle_connection_with_timeout(tls_stream, kv, pw, timeout).await
                            }
                            Err(e) => {
                                tracing::debug!("RESP TLS handshake failed from {addr}: {e}");
                                Err(e)
                            }
                        }
                    } else {
                        handle_connection_with_timeout(stream, kv, pw, timeout).await
                    };

                    if let Err(e) = result {
                        tracing::debug!("RESP connection from {} closed: {}", addr, e);
                    }
                    conn_counter.fetch_sub(1, Ordering::Relaxed);
                });
            }
            _ = shutdown.notified() => {
                tracing::info!("RESP server shutting down");
                break;
            }
        }
    }
    Ok(())
}

/// Handle a single connection with an idle timeout wrapper.
async fn handle_connection_with_timeout<S: AsyncRead + AsyncWrite + Unpin>(
    stream: S,
    kv: Arc<KvStore>,
    password: Option<String>,
    idle_timeout: std::time::Duration,
) -> std::io::Result<()> {
    let (reader, mut writer) = tokio::io::split(stream);
    let mut buf_reader = BufReader::new(reader);
    let mut handler = super::handler::RespHandler::new(kv, password);

    loop {
        // Apply idle timeout to each read
        let value = match tokio::time::timeout(idle_timeout, super::parser::read_value(&mut buf_reader)).await {
            Ok(result) => result?,
            Err(_) => {
                // Idle timeout — close connection
                tracing::debug!("RESP connection idle timeout, closing");
                return Ok(());
            }
        };

        let args = match super::parser::parse_command(value) {
            Some(args) => args,
            None => {
                use tokio::io::AsyncWriteExt;
                writer
                    .write_all(&super::encoder::encode_error("ERR invalid command format"))
                    .await?;
                continue;
            }
        };

        // Check for QUIT command.
        if !args.is_empty() {
            let cmd = String::from_utf8_lossy(&args[0]).to_uppercase();
            if cmd == "QUIT" {
                use tokio::io::AsyncWriteExt;
                writer
                    .write_all(&super::encoder::encode_simple_string("OK"))
                    .await?;
                break;
            }
        }

        let response = handler.handle_command(args);
        use tokio::io::AsyncWriteExt;
        writer.write_all(&response).await?;
        writer.flush().await?;
    }
    Ok(())
}
