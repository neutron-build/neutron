//! RESP2 TCP server.
//!
//! Listens for Redis-compatible connections and dispatches commands to the
//! [`RespHandler`]. Each connection gets its own handler instance so
//! authentication state is per-connection.
//!
//! Supports optional TLS, connection limits, idle timeouts, and Pub/Sub.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

use super::pubsub_registry::PubSubRegistry;
use crate::kv::KvStore;

/// Configuration for the RESP server.
pub struct RespServerConfig {
    /// Maximum concurrent connections (default 1024).
    pub max_connections: usize,
    /// Idle timeout in seconds — connections with no activity are closed (default 300).
    pub idle_timeout_secs: u64,
    /// Optional TLS acceptor. When present, all connections are TLS-encrypted
    /// (NE-10: the SQL TLS acceptor is routed here so RESP AUTH never rides
    /// plaintext on a TLS-enabled deployment).
    pub tls_config: Option<pgwire::tokio::TlsAcceptor>,
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

/// RAII connection-slot lease (NE-08).
///
/// The counter used to be decremented manually at the end of the spawned
/// connection task; a panic anywhere in the handshake/parse path skipped the
/// decrement and permanently consumed one of the `max_connections` slots.
/// Constructing this guard immediately after admission makes `Drop` release
/// the slot on every exit path — normal return, error, cancellation and
/// panic unwind alike.
struct ConnectionSlotGuard(Arc<AtomicUsize>);

impl Drop for ConnectionSlotGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Start the RESP2 server, accepting connections until `shutdown` is notified.
pub async fn start_resp_server(
    bind_addr: String,
    kv: Arc<KvStore>,
    password: Option<String>,
    shutdown: Arc<tokio::sync::Notify>,
) -> std::io::Result<()> {
    start_resp_server_with_config(
        bind_addr,
        kv,
        password,
        shutdown,
        RespServerConfig::default(),
    )
    .await
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
    serve_resp_connections(listener, kv, password, shutdown, config).await
}

/// Connection loop over an already-bound listener.
///
/// Split out of [`start_resp_server_with_config`] so tests can bind their own
/// ephemeral loopback listener and still exercise admission, slot accounting
/// and shutdown with the production loop.
pub async fn serve_resp_connections(
    listener: TcpListener,
    kv: Arc<KvStore>,
    password: Option<String>,
    shutdown: Arc<tokio::sync::Notify>,
    config: RespServerConfig,
) -> std::io::Result<()> {
    let active_connections = Arc::new(AtomicUsize::new(0));
    let max_connections = config.max_connections;
    let idle_timeout = std::time::Duration::from_secs(config.idle_timeout_secs);
    let pubsub = Arc::new(PubSubRegistry::new());

    let tls_acceptor = config.tls_config;

    if tls_acceptor.is_some() {
        tracing::info!(
            "RESP server listening on {} (TLS enabled)",
            listener.local_addr().map(|a| a.to_string()).unwrap_or_default()
        );
    } else {
        tracing::info!(
            "RESP server listening on {}",
            listener.local_addr().map(|a| a.to_string()).unwrap_or_default()
        );
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
                let slot = ConnectionSlotGuard(Arc::clone(&active_connections));
                let kv = Arc::clone(&kv);
                let pw = password.clone();
                let tls = tls_acceptor.clone();
                let timeout = idle_timeout;
                let pubsub = Arc::clone(&pubsub);

                tokio::spawn(async move {
                    // Released on every exit path, including a panic anywhere
                    // below (NE-08): the guard drops with the task.
                    let _slot = slot;
                    let result = if let Some(acceptor) = tls {
                        match acceptor.accept(stream).await {
                            Ok(tls_stream) => {
                                handle_connection_with_timeout(tls_stream, kv, pw, timeout, pubsub).await
                            }
                            Err(e) => {
                                tracing::debug!("RESP TLS handshake failed from {addr}: {e}");
                                Err(e)
                            }
                        }
                    } else {
                        handle_connection_with_timeout(stream, kv, pw, timeout, pubsub).await
                    };

                    if let Err(e) = result {
                        tracing::debug!("RESP connection from {} closed: {}", addr, e);
                    }
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
    pubsub: Arc<PubSubRegistry>,
) -> std::io::Result<()> {
    let (reader, mut writer) = tokio::io::split(stream);
    let mut buf_reader = BufReader::new(reader);
    let mut handler = super::handler::RespHandler::new(kv, password, Arc::clone(&pubsub));

    loop {
        // If we are in pub/sub mode, select between incoming commands and
        // subscription messages.
        if handler.is_in_pubsub_mode() {
            tokio::select! {
                // Incoming command from client
                read_result = tokio::time::timeout(idle_timeout, super::parser::read_value(&mut buf_reader)) => {
                    let value = match read_result {
                        Ok(result) => result?,
                        Err(_) => {
                            tracing::debug!("RESP connection idle timeout, closing");
                            handler.cleanup_pubsub();
                            return Ok(());
                        }
                    };

                    let args = match super::parser::parse_command(value) {
                        Some(args) => args,
                        None => {
                            writer.write_all(&super::encoder::encode_error("ERR invalid command format")).await?;
                            continue;
                        }
                    };

                    if !args.is_empty() {
                        let cmd = String::from_utf8_lossy(&args[0]).to_uppercase();
                        if cmd == "QUIT" {
                            writer.write_all(&super::encoder::encode_simple_string("OK")).await?;
                            handler.cleanup_pubsub();
                            break;
                        }
                    }

                    // In pub/sub mode, only SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE,
                    // PUNSUBSCRIBE, PING, and QUIT are allowed.
                    // Defense-in-depth: this loop is only reachable after an
                    // authenticated SUBSCRIBE below, but any future
                    // mode-entry refactor would reopen the hole this closes.
                    if !handler.is_authenticated() {
                        writer
                            .write_all(&super::encoder::encode_error(
                                "NOAUTH Authentication required.",
                            ))
                            .await?;
                        writer.flush().await?;
                        continue;
                    }
                    let responses = handler.handle_pubsub_command(args);
                    for resp in responses {
                        writer.write_all(&resp).await?;
                    }
                    writer.flush().await?;
                }
                // Subscription message to push to client
                msg = handler.recv_pubsub_message() => {
                    match msg {
                        Some(data) => {
                            writer.write_all(&data).await?;
                            writer.flush().await?;
                        }
                        None => {
                            // The registry dropped this subscriber's sender.
                            // The only reason it does that while the connection
                            // is still in pub/sub mode is the output-buffer
                            // limit (S31-08): the client was not draining, so
                            // it is disconnected the way Redis disconnects it.
                            // Returning here rather than continuing also avoids
                            // spinning on a closed channel.
                            tracing::warn!(
                                "RESP subscriber disconnected: pub/sub output buffer limit exceeded"
                            );
                            handler.cleanup_pubsub();
                            return Ok(());
                        }
                    }
                }
            }
        } else {
            // Normal (non-pub/sub) mode
            let value = match tokio::time::timeout(
                idle_timeout,
                super::parser::read_value(&mut buf_reader),
            )
            .await
            {
                Ok(result) => result?,
                Err(_) => {
                    tracing::debug!("RESP connection idle timeout, closing");
                    return Ok(());
                }
            };

            let args = match super::parser::parse_command(value) {
                Some(args) => args,
                None => {
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
                    writer
                        .write_all(&super::encoder::encode_simple_string("OK"))
                        .await?;
                    break;
                }
            }

            // Check if this is a pub/sub command that should enter pub/sub mode
            if !args.is_empty() {
                let cmd = String::from_utf8_lossy(&args[0]).to_uppercase();
                match cmd.as_str() {
                    "SUBSCRIBE" | "PSUBSCRIBE" => {
                        // The intercept below calls handle_pubsub_command
                        // directly, bypassing handle_command's NOAUTH gate —
                        // and once in pub/sub mode EVERY command flowed
                        // through the ungated path. Gate it here.
                        if !handler.is_authenticated() {
                            writer
                                .write_all(&super::encoder::encode_error(
                                    "NOAUTH Authentication required.",
                                ))
                                .await?;
                            writer.flush().await?;
                            continue;
                        }
                        let responses = handler.handle_pubsub_command(args);
                        for resp in responses {
                            writer.write_all(&resp).await?;
                        }
                        writer.flush().await?;
                        continue;
                    }
                    "PUBLISH" => {
                        let response = handler.handle_command(args);
                        writer.write_all(&response).await?;
                        writer.flush().await?;
                        continue;
                    }
                    _ => {}
                }
            }

            let response = handler.handle_command(args);
            writer.write_all(&response).await?;
            writer.flush().await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn malformed_prefixes_do_not_leak_connection_slots() {
        // NE-08 regression: a two-byte non-ASCII prefix used to panic the
        // connection task inside the parser; the manual counter decrement at
        // the end of the task never ran, permanently consuming a slot. With
        // a two-slot server, three sequential malformed connections exhaust
        // admission and a subsequent valid PING is refused. The RAII slot
        // guard (plus the parser fix) must keep all slots releasable.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let kv = Arc::new(KvStore::new());
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let config = RespServerConfig {
            max_connections: 2,
            idle_timeout_secs: 5,
            tls_config: None,
        };
        let server = tokio::spawn(serve_resp_connections(
            listener,
            kv,
            None,
            shutdown.clone(),
            config,
        ));

        // Three malformed connections against two slots. Each must be
        // answered with a protocol error (or closed) and release its slot.
        for _ in 0..3 {
            let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
            use tokio::io::AsyncWriteExt;
            sock.write_all("é\r\n".as_bytes()).await.unwrap();
            // Give the task time to run and exit; then close our side.
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            drop(sock);
        }

        // The slots must all be back: a valid PING on a fresh connection
        // must be served, not silently dropped by an exhausted admission
        // limit (an over-limit socket is dropped before any read, yielding
        // an immediate clean close with zero response bytes).
        let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
        sock.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
        let mut buf = [0u8; 64];
        let read = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            tokio::io::AsyncReadExt::read(&mut sock, &mut buf),
        )
        .await;
        match read {
            Ok(Ok(0)) => panic!("valid PING connection closed without an answer (slot leak)"),
            Ok(Ok(n)) => {
                let body = String::from_utf8_lossy(&buf[..n]).to_string();
                assert!(body.contains("+PONG"), "expected +PONG, got: {body:?}");
            }
            Ok(Err(e)) => panic!("valid PING connection failed: {e}"),
            Err(_) => panic!("valid PING connection never answered"),
        }

        shutdown.notify_waiters();
        tokio::time::timeout(std::time::Duration::from_secs(5), server)
            .await
            .expect("server exits on shutdown")
            .expect("server loop clean");
    }
}
