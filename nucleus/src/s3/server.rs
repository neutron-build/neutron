//! S3 gateway TCP server.
//!
//! Accepts HTTP/1.1 connections and dispatches S3 operations to the
//! [`S3Handler`]. Mirrors the RESP server's shape: connection cap, idle
//! timeout, graceful shutdown via `Notify`.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

use crate::executor::Executor;

use super::handlers::{S3Config, S3Handler};
use super::http::{ReadOutcome, Response, read_request_body, read_request_head, write_response};

/// Configuration for the S3 server.
pub struct S3ServerConfig {
    /// Maximum concurrent connections (default 256).
    pub max_connections: usize,
    /// Idle timeout in seconds (default 300).
    pub idle_timeout_secs: u64,
}

impl Default for S3ServerConfig {
    fn default() -> Self {
        Self {
            max_connections: 256,
            idle_timeout_secs: 300,
        }
    }
}

/// Start the S3 gateway, accepting connections until `shutdown` is notified.
pub async fn start_s3_server(
    bind_addr: String,
    executor: Arc<Executor>,
    config: Arc<S3Config>,
    shutdown: Arc<tokio::sync::Notify>,
) -> std::io::Result<()> {
    start_s3_server_with_config(
        bind_addr,
        executor,
        config,
        shutdown,
        S3ServerConfig::default(),
    )
    .await
}

pub async fn start_s3_server_with_config(
    bind_addr: String,
    executor: Arc<Executor>,
    config: Arc<S3Config>,
    shutdown: Arc<tokio::sync::Notify>,
    server_config: S3ServerConfig,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(&bind_addr).await?;
    tracing::info!("S3 gateway listening on {bind_addr}");
    serve(listener, executor, config, shutdown, server_config).await
}

/// Serve S3 on an already-bound listener (lets callers bind port 0 and read
/// the assigned port back — used by the integration tests).
pub async fn serve(
    listener: TcpListener,
    executor: Arc<Executor>,
    config: Arc<S3Config>,
    shutdown: Arc<tokio::sync::Notify>,
    server_config: S3ServerConfig,
) -> std::io::Result<()> {
    let connections = Arc::new(AtomicUsize::new(0));
    let handler = Arc::new(S3Handler::new(executor, config));
    // Body cap: the max object size plus aws-chunked framing overhead.
    let max_body = handler.max_object_bytes() + 128 * 1024;

    loop {
        let (stream, peer) = tokio::select! {
            accepted = listener.accept() => accepted?,
            _ = shutdown.notified() => {
                tracing::info!("S3 gateway shutting down");
                return Ok(());
            }
        };

        if connections.load(Ordering::Relaxed) >= server_config.max_connections {
            drop(stream);
            continue;
        }
        connections.fetch_add(1, Ordering::Relaxed);
        let connections = Arc::clone(&connections);
        let handler = Arc::clone(&handler);
        let idle = std::time::Duration::from_secs(server_config.idle_timeout_secs);

        tokio::spawn(async move {
            let _ = stream.set_nodelay(true);
            let (read_half, mut write_half) = stream.into_split();
            let mut reader = BufReader::new(read_half);

            loop {
                let head = match tokio::time::timeout(
                    idle,
                    read_request_head(&mut reader, max_body),
                )
                .await
                {
                    Err(_) | Ok(Err(_)) => break, // idle timeout or socket error
                    Ok(Ok(ReadOutcome::Closed)) => break,
                    Ok(Ok(ReadOutcome::Bad(msg))) => {
                        let resp = Response::with_body(400, "text/plain", msg.as_bytes().to_vec());
                        let _ = write_response(&mut write_half, &resp, false, false).await;
                        break;
                    }
                    Ok(Ok(ReadOutcome::Head(head))) => head,
                };

                if head.expects_continue && head.content_length > 0 {
                    if write_half
                        .write_all(b"HTTP/1.1 100 Continue\r\n\r\n")
                        .await
                        .is_err()
                    {
                        break;
                    }
                    let _ = write_half.flush().await;
                }

                let req =
                    match tokio::time::timeout(idle, read_request_body(&mut reader, *head)).await {
                        Ok(Ok(req)) => req,
                        _ => break,
                    };
                let keep_alive = req.keep_alive;
                let head_only = req.method == "HEAD";

                let resp = handler.handle(req);
                if write_response(&mut write_half, &resp, keep_alive, head_only)
                    .await
                    .is_err()
                    || !keep_alive
                {
                    break;
                }
            }
            connections.fetch_sub(1, Ordering::Relaxed);
            tracing::debug!("S3 connection from {peer} closed");
        });
    }
}
