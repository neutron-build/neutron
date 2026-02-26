//! WebSocket support.
//!
//! # Example
//!
//! ```rust,ignore
//! use neutron::prelude::*;
//! use neutron::ws::{WebSocketUpgrade, WebSocket, Message};
//!
//! async fn ws_handler(ws: WebSocketUpgrade) -> Response {
//!     ws.on_upgrade(|mut socket| async move {
//!         while let Some(msg) = socket.recv().await {
//!             match msg {
//!                 Message::Text(text) => {
//!                     socket.send(Message::text(format!("Echo: {text}"))).await.ok();
//!                 }
//!                 Message::Close(_) => break,
//!                 _ => {}
//!             }
//!         }
//!     })
//! }
//!
//! let router = Router::new().get("/ws", ws_handler);
//! ```

use std::future::Future;

use base64::Engine;
use http::{HeaderValue, StatusCode};
use hyper_util::rt::TokioIo;
use sha1::{Digest, Sha1};

use crate::extract::FromRequest;
use crate::handler::{Body, IntoResponse, Request, Response};

/// The WebSocket GUID from RFC 6455.
const WS_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

// ---------------------------------------------------------------------------
// Message types
// ---------------------------------------------------------------------------

/// A WebSocket message.
#[derive(Debug, Clone)]
pub enum Message {
    /// A UTF-8 text message.
    Text(String),
    /// A binary message.
    Binary(Vec<u8>),
    /// A ping control frame.
    Ping(Vec<u8>),
    /// A pong control frame.
    Pong(Vec<u8>),
    /// A close control frame with optional code and reason.
    Close(Option<CloseFrame>),
}

impl Message {
    /// Create a text message.
    pub fn text(s: impl Into<String>) -> Self {
        Message::Text(s.into())
    }

    /// Create a binary message.
    pub fn binary(data: impl Into<Vec<u8>>) -> Self {
        Message::Binary(data.into())
    }

    /// Create a close message with code and reason.
    pub fn close(code: u16, reason: impl Into<String>) -> Self {
        Message::Close(Some(CloseFrame {
            code,
            reason: reason.into(),
        }))
    }

    /// Returns `true` if this is a text message.
    pub fn is_text(&self) -> bool {
        matches!(self, Message::Text(_))
    }

    /// Returns `true` if this is a binary message.
    pub fn is_binary(&self) -> bool {
        matches!(self, Message::Binary(_))
    }

    /// Returns `true` if this is a close message.
    pub fn is_close(&self) -> bool {
        matches!(self, Message::Close(_))
    }

    /// Get the message as text, if it is a text message.
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Message::Text(s) => Some(s),
            _ => None,
        }
    }

    /// Consume the message into raw bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        match self {
            Message::Text(s) => s.into_bytes(),
            Message::Binary(b) | Message::Ping(b) | Message::Pong(b) => b,
            Message::Close(_) => Vec::new(),
        }
    }
}

/// Close frame with code and reason.
#[derive(Debug, Clone)]
pub struct CloseFrame {
    /// The close code (e.g., 1000 for normal closure).
    pub code: u16,
    /// The close reason.
    pub reason: String,
}

// ---------------------------------------------------------------------------
// WebSocket error
// ---------------------------------------------------------------------------

/// Error type for WebSocket operations.
#[derive(Debug)]
pub struct WsError {
    message: String,
}

impl std::fmt::Display for WsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for WsError {}

impl From<fastwebsockets::WebSocketError> for WsError {
    fn from(e: fastwebsockets::WebSocketError) -> Self {
        WsError {
            message: format!("{e:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// WebSocketUpgrade extractor
// ---------------------------------------------------------------------------

/// Extractor that validates WebSocket upgrade requests.
///
/// When extracted, this validates that the request has the proper WebSocket
/// upgrade headers. Call [`on_upgrade`](Self::on_upgrade) to complete the
/// handshake and obtain a [`WebSocket`] connection.
pub struct WebSocketUpgrade {
    on_upgrade: hyper::upgrade::OnUpgrade,
    sec_websocket_key: HeaderValue,
    protocols: Option<HeaderValue>,
}

impl WebSocketUpgrade {
    /// Complete the upgrade and invoke the callback with the WebSocket.
    ///
    /// Returns a `101 Switching Protocols` response. The callback runs in a
    /// spawned task after the HTTP upgrade completes.
    pub fn on_upgrade<F, Fut>(self, callback: F) -> Response
    where
        F: FnOnce(WebSocket) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let accept_key = compute_accept_key(&self.sec_websocket_key);

        tokio::spawn(async move {
            match self.on_upgrade.await {
                Ok(upgraded) => {
                    let io = TokioIo::new(upgraded);
                    let ws =
                        fastwebsockets::WebSocket::after_handshake(io, fastwebsockets::Role::Server);
                    let ws = fastwebsockets::FragmentCollector::new(ws);
                    let socket = WebSocket { inner: ws };
                    callback(socket).await;
                }
                Err(e) => {
                    tracing::error!("WebSocket upgrade failed: {e}");
                }
            }
        });

        let mut builder = http::Response::builder()
            .status(StatusCode::SWITCHING_PROTOCOLS)
            .header("connection", "upgrade")
            .header("upgrade", "websocket")
            .header("sec-websocket-accept", accept_key);

        if let Some(protocol) = self.protocols {
            builder = builder.header("sec-websocket-protocol", protocol);
        }

        builder.body(Body::empty()).unwrap()
    }

    /// Negotiate a subprotocol from the client's `Sec-WebSocket-Protocol` header.
    ///
    /// Checks the client's requested protocols against the provided list and
    /// selects the first match. The selected protocol is included in the
    /// response headers.
    pub fn protocols(mut self, supported: &[&str]) -> Self {
        if let Some(ref requested) = self.protocols {
            if let Ok(requested_str) = requested.to_str() {
                for &supported_proto in supported {
                    for requested_proto in requested_str.split(',').map(str::trim) {
                        if requested_proto.eq_ignore_ascii_case(supported_proto) {
                            self.protocols =
                                Some(HeaderValue::from_str(supported_proto).unwrap());
                            return self;
                        }
                    }
                }
            }
        }
        self.protocols = None;
        self
    }
}

impl FromRequest for WebSocketUpgrade {
    fn from_request(req: &Request) -> Result<Self, Response> {
        // Connection: upgrade
        let connection = req
            .headers()
            .get("connection")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        if !connection
            .split(',')
            .any(|token| token.trim().eq_ignore_ascii_case("upgrade"))
        {
            return Err(
                (StatusCode::BAD_REQUEST, "Missing Connection: upgrade header").into_response(),
            );
        }

        // Upgrade: websocket
        let upgrade = req
            .headers()
            .get("upgrade")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        if !upgrade.eq_ignore_ascii_case("websocket") {
            return Err(
                (StatusCode::BAD_REQUEST, "Missing Upgrade: websocket header").into_response(),
            );
        }

        // Sec-WebSocket-Version: 13
        let version = req
            .headers()
            .get("sec-websocket-version")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        if version != "13" {
            return Err(
                (StatusCode::BAD_REQUEST, "Unsupported WebSocket version").into_response(),
            );
        }

        // Sec-WebSocket-Key
        let key = req
            .headers()
            .get("sec-websocket-key")
            .cloned()
            .ok_or_else(|| {
                (StatusCode::BAD_REQUEST, "Missing Sec-WebSocket-Key").into_response()
            })?;

        // Extract the upgrade future
        let on_upgrade = req.take_on_upgrade().ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                "Connection does not support upgrade",
            )
                .into_response()
        })?;

        // Optional: Sec-WebSocket-Protocol
        let protocols = req.headers().get("sec-websocket-protocol").cloned();

        Ok(WebSocketUpgrade {
            on_upgrade,
            sec_websocket_key: key,
            protocols,
        })
    }
}

// ---------------------------------------------------------------------------
// WebSocket connection
// ---------------------------------------------------------------------------

/// An active WebSocket connection.
///
/// Use [`recv`](Self::recv) to receive messages and [`send`](Self::send) to
/// send them. For concurrent read/write, use [`split`](Self::split).
pub struct WebSocket {
    inner: fastwebsockets::FragmentCollector<TokioIo<hyper::upgrade::Upgraded>>,
}

impl WebSocket {
    /// Receive the next message.
    ///
    /// Returns `None` when the connection is closed or an error occurs.
    pub async fn recv(&mut self) -> Option<Message> {
        match self.inner.read_frame().await {
            Ok(frame) => match frame.opcode {
                fastwebsockets::OpCode::Text => {
                    String::from_utf8(frame.payload.to_vec())
                        .ok()
                        .map(Message::Text)
                }
                fastwebsockets::OpCode::Binary => {
                    Some(Message::Binary(frame.payload.to_vec()))
                }
                fastwebsockets::OpCode::Ping => Some(Message::Ping(frame.payload.to_vec())),
                fastwebsockets::OpCode::Pong => Some(Message::Pong(frame.payload.to_vec())),
                fastwebsockets::OpCode::Close => {
                    let close = if frame.payload.len() >= 2 {
                        let code =
                            u16::from_be_bytes([frame.payload[0], frame.payload[1]]);
                        let reason =
                            String::from_utf8_lossy(&frame.payload[2..]).into_owned();
                        Some(CloseFrame { code, reason })
                    } else {
                        None
                    };
                    Some(Message::Close(close))
                }
                fastwebsockets::OpCode::Continuation => None,
            },
            Err(_) => None,
        }
    }

    /// Send a message.
    pub async fn send(&mut self, msg: Message) -> Result<(), WsError> {
        let frame = match msg {
            Message::Text(text) => {
                fastwebsockets::Frame::text(fastwebsockets::Payload::Owned(text.into_bytes()))
            }
            Message::Binary(data) => {
                fastwebsockets::Frame::binary(fastwebsockets::Payload::Owned(data))
            }
            Message::Ping(data) => fastwebsockets::Frame::new(
                true,
                fastwebsockets::OpCode::Ping,
                None,
                fastwebsockets::Payload::Owned(data),
            ),
            Message::Pong(data) => {
                fastwebsockets::Frame::pong(fastwebsockets::Payload::Owned(data))
            }
            Message::Close(cf) => {
                if let Some(cf) = cf {
                    fastwebsockets::Frame::close(cf.code, cf.reason.as_bytes())
                } else {
                    fastwebsockets::Frame::close(1000, b"")
                }
            }
        };
        self.inner.write_frame(frame).await.map_err(WsError::from)
    }

    /// Close the connection gracefully with code 1000 (normal closure).
    pub async fn close(mut self) -> Result<(), WsError> {
        self.inner
            .write_frame(fastwebsockets::Frame::close(1000, b""))
            .await
            .map_err(WsError::from)
    }

    /// Split into independent sender and receiver for concurrent operations.
    ///
    /// Internally spawns a coordinator task that multiplexes reads and writes.
    /// The [`WsSender`] is cheaply cloneable for broadcasting from multiple tasks.
    pub fn split(self) -> (WsSender, WsReceiver) {
        let (send_tx, mut send_rx) = tokio::sync::mpsc::channel::<Message>(32);
        let (recv_tx, recv_rx) = tokio::sync::mpsc::unbounded_channel::<Message>();

        tokio::spawn(async move {
            let mut ws = self.inner;
            loop {
                tokio::select! {
                    frame = ws.read_frame() => {
                        match frame {
                            Ok(frame) => {
                                let msg = match frame.opcode {
                                    fastwebsockets::OpCode::Text => {
                                        String::from_utf8(frame.payload.to_vec())
                                            .ok()
                                            .map(Message::Text)
                                    }
                                    fastwebsockets::OpCode::Binary => {
                                        Some(Message::Binary(frame.payload.to_vec()))
                                    }
                                    fastwebsockets::OpCode::Close => {
                                        let close = if frame.payload.len() >= 2 {
                                            let code = u16::from_be_bytes([
                                                frame.payload[0],
                                                frame.payload[1],
                                            ]);
                                            let reason = String::from_utf8_lossy(
                                                &frame.payload[2..],
                                            )
                                            .into_owned();
                                            Some(CloseFrame { code, reason })
                                        } else {
                                            None
                                        };
                                        Some(Message::Close(close))
                                    }
                                    fastwebsockets::OpCode::Ping => {
                                        Some(Message::Ping(frame.payload.to_vec()))
                                    }
                                    fastwebsockets::OpCode::Pong => {
                                        Some(Message::Pong(frame.payload.to_vec()))
                                    }
                                    _ => None,
                                };
                                if let Some(msg) = msg {
                                    let is_close = msg.is_close();
                                    if recv_tx.send(msg).is_err() {
                                        break;
                                    }
                                    if is_close {
                                        break;
                                    }
                                }
                            }
                            Err(_) => break,
                        }
                    }
                    msg = send_rx.recv() => {
                        match msg {
                            Some(msg) => {
                                let frame = match msg {
                                    Message::Text(text) => {
                                        fastwebsockets::Frame::text(
                                            fastwebsockets::Payload::Owned(text.into_bytes()),
                                        )
                                    }
                                    Message::Binary(data) => {
                                        fastwebsockets::Frame::binary(
                                            fastwebsockets::Payload::Owned(data),
                                        )
                                    }
                                    Message::Ping(data) => fastwebsockets::Frame::new(
                                        true,
                                        fastwebsockets::OpCode::Ping,
                                        None,
                                        fastwebsockets::Payload::Owned(data),
                                    ),
                                    Message::Pong(data) => fastwebsockets::Frame::pong(
                                        fastwebsockets::Payload::Owned(data),
                                    ),
                                    Message::Close(cf) => {
                                        if let Some(cf) = cf {
                                            fastwebsockets::Frame::close(
                                                cf.code,
                                                cf.reason.as_bytes(),
                                            )
                                        } else {
                                            fastwebsockets::Frame::close(1000, b"")
                                        }
                                    }
                                };
                                if ws.write_frame(frame).await.is_err() {
                                    break;
                                }
                            }
                            None => break,
                        }
                    }
                }
            }
        });

        (WsSender { tx: send_tx }, WsReceiver { rx: recv_rx })
    }
}

/// Sending half of a split WebSocket. Cloneable for broadcasting.
#[derive(Clone)]
pub struct WsSender {
    tx: tokio::sync::mpsc::Sender<Message>,
}

impl WsSender {
    /// Send a message. Returns an error if the connection is closed.
    pub async fn send(&self, msg: Message) -> Result<(), WsError> {
        self.tx.send(msg).await.map_err(|_| WsError {
            message: "WebSocket connection closed".into(),
        })
    }
}

/// Receiving half of a split WebSocket.
pub struct WsReceiver {
    rx: tokio::sync::mpsc::UnboundedReceiver<Message>,
}

impl WsReceiver {
    /// Receive the next message. Returns `None` when closed.
    pub async fn recv(&mut self) -> Option<Message> {
        self.rx.recv().await
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Compute `Sec-WebSocket-Accept` from the client's `Sec-WebSocket-Key`.
fn compute_accept_key(key: &HeaderValue) -> String {
    let mut hasher = Sha1::new();
    hasher.update(key.as_bytes());
    hasher.update(WS_GUID.as_bytes());
    let hash = hasher.finalize();
    base64::engine::general_purpose::STANDARD.encode(hash)
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::router::Router;
    use crate::testing::TestClient;

    // -----------------------------------------------------------------------
    // Accept key computation
    // -----------------------------------------------------------------------

    #[test]
    fn accept_key_rfc_example() {
        // RFC 6455 Section 4.2.2 example
        let key = HeaderValue::from_static("dGhlIHNhbXBsZSBub25jZQ==");
        let accept = compute_accept_key(&key);
        assert_eq!(accept, "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=");
    }

    // -----------------------------------------------------------------------
    // Message type
    // -----------------------------------------------------------------------

    #[test]
    fn message_text() {
        let msg = Message::text("hello");
        assert!(msg.is_text());
        assert!(!msg.is_binary());
        assert!(!msg.is_close());
        assert_eq!(msg.as_text(), Some("hello"));
    }

    #[test]
    fn message_binary() {
        let msg = Message::binary(vec![1, 2, 3]);
        assert!(msg.is_binary());
        assert!(!msg.is_text());
        assert_eq!(msg.as_text(), None);
    }

    #[test]
    fn message_close() {
        let msg = Message::close(1000, "bye");
        assert!(msg.is_close());
        if let Message::Close(Some(frame)) = &msg {
            assert_eq!(frame.code, 1000);
            assert_eq!(frame.reason, "bye");
        } else {
            panic!("expected close frame");
        }
    }

    #[test]
    fn message_into_bytes() {
        let msg = Message::text("hello");
        assert_eq!(msg.into_bytes(), b"hello");

        let msg = Message::binary(vec![1, 2, 3]);
        assert_eq!(msg.into_bytes(), vec![1, 2, 3]);
    }

    // -----------------------------------------------------------------------
    // WebSocketUpgrade extractor — header validation
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn upgrade_rejects_missing_connection() {
        let client = TestClient::new(Router::new().get("/ws", |ws: WebSocketUpgrade| async {
            ws.on_upgrade(|_| async {})
        }));

        let resp = client.get("/ws").send().await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert!(resp.text().await.contains("Connection: upgrade"));
    }

    #[tokio::test]
    async fn upgrade_rejects_missing_upgrade_header() {
        let client = TestClient::new(Router::new().get("/ws", |ws: WebSocketUpgrade| async {
            ws.on_upgrade(|_| async {})
        }));

        let resp = client
            .get("/ws")
            .header("connection", "upgrade")
            .send()
            .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert!(resp.text().await.contains("Upgrade: websocket"));
    }

    #[tokio::test]
    async fn upgrade_rejects_wrong_version() {
        let client = TestClient::new(Router::new().get("/ws", |ws: WebSocketUpgrade| async {
            ws.on_upgrade(|_| async {})
        }));

        let resp = client
            .get("/ws")
            .header("connection", "upgrade")
            .header("upgrade", "websocket")
            .header("sec-websocket-version", "8")
            .send()
            .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert!(resp.text().await.contains("version"));
    }

    #[tokio::test]
    async fn upgrade_rejects_missing_key() {
        let client = TestClient::new(Router::new().get("/ws", |ws: WebSocketUpgrade| async {
            ws.on_upgrade(|_| async {})
        }));

        let resp = client
            .get("/ws")
            .header("connection", "upgrade")
            .header("upgrade", "websocket")
            .header("sec-websocket-version", "13")
            .send()
            .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert!(resp.text().await.contains("Sec-WebSocket-Key"));
    }

    #[tokio::test]
    async fn upgrade_rejects_no_upgrade_available() {
        // TestClient doesn't provide an upgrade connection, so after passing
        // header validation, extraction fails with "does not support upgrade".
        let client = TestClient::new(Router::new().get("/ws", |ws: WebSocketUpgrade| async {
            ws.on_upgrade(|_| async {})
        }));

        let resp = client
            .get("/ws")
            .header("connection", "upgrade")
            .header("upgrade", "websocket")
            .header("sec-websocket-version", "13")
            .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
            .send()
            .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert!(resp.text().await.contains("does not support upgrade"));
    }

    // -----------------------------------------------------------------------
    // Protocol negotiation
    // -----------------------------------------------------------------------

    #[test]
    fn protocol_negotiation() {
        let key = HeaderValue::from_static("dGhlIHNhbXBsZSBub25jZQ==");
        // Simulate a WebSocketUpgrade with protocols header
        // (we can't fully construct one without OnUpgrade, so test the
        // protocol selection logic separately)
        let requested = HeaderValue::from_static("graphql-ws, graphql-transport-ws");
        let mut found = None;

        let requested_str = requested.to_str().unwrap();
        let supported = &["graphql-transport-ws", "graphql-ws"];
        for &proto in supported {
            for req_proto in requested_str.split(',').map(str::trim) {
                if req_proto.eq_ignore_ascii_case(proto) {
                    found = Some(proto);
                    break;
                }
            }
            if found.is_some() {
                break;
            }
        }

        // Should match the first supported protocol, not the first requested
        assert_eq!(found, Some("graphql-transport-ws"));
        let _ = key;
    }

    // -----------------------------------------------------------------------
    // WsError
    // -----------------------------------------------------------------------

    #[test]
    fn ws_error_display() {
        let err = WsError {
            message: "test error".into(),
        };
        assert_eq!(format!("{err}"), "test error");
    }
}
