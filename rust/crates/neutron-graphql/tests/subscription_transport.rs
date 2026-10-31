use futures_util::Stream;
use neutron::{app::Neutron, router::Router};
use neutron_graphql::{
    graphql_subscription_handler, GraphQlRequest, GraphQlResponse, SubscriptionSchema,
};
use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::oneshot,
};

struct Pending(Arc<AtomicBool>);
impl Drop for Pending {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}
impl Stream for Pending {
    type Item = GraphQlResponse;
    fn poll_next(
        self: Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::task::Poll::Pending
    }
}
struct Schema(Arc<AtomicBool>);
impl SubscriptionSchema for Schema {
    fn subscribe(
        self: Arc<Self>,
        req: GraphQlRequest,
    ) -> Pin<Box<dyn Stream<Item = GraphQlResponse> + Send>> {
        if req.query == "idle" {
            Box::pin(Pending(self.0.clone()))
        } else {
            Box::pin(futures_util::stream::once(async {
                GraphQlResponse::ok(serde_json::json!({"count": 1}))
            }))
        }
    }
}
async fn send(stream: &mut TcpStream, json: &str) {
    let bytes = json.as_bytes();
    assert!(bytes.len() < 126);
    let mut frame = vec![0x81, 0x80 | bytes.len() as u8, 1, 2, 3, 4];
    frame.extend(
        bytes
            .iter()
            .enumerate()
            .map(|(i, b)| b ^ [1, 2, 3, 4][i % 4]),
    );
    stream.write_all(&frame).await.unwrap();
}
async fn recv(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut head = [0; 2];
    stream.read_exact(&mut head).await.unwrap();
    let length = match head[1] & 127 {
        126 => stream.read_u16().await.unwrap() as usize,
        127 => stream.read_u64().await.unwrap() as usize,
        length => length as usize,
    };
    let mut body = vec![0; length];
    stream.read_exact(&mut body).await.unwrap();
    (head[0] & 15, body)
}

#[tokio::test]
async fn modern_protocol_multiplexes_cancels_idle_and_handles_ping() {
    let dropped = Arc::new(AtomicBool::new(false));
    let schema = Arc::new(Schema(dropped.clone()));
    let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = reservation.local_addr().unwrap();
    drop(reservation);
    let (stop, stopped) = oneshot::channel();
    let server = tokio::spawn(async move {
        Neutron::new()
            .router(Router::new().get("/ws", graphql_subscription_handler(schema)))
            .shutdown_signal(async move {
                let _ = stopped.await;
            })
            .listen(addr)
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut socket = TcpStream::connect(addr).await.unwrap();
    socket.write_all(b"GET /ws HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Protocol: graphql-transport-ws\r\n\r\n").await.unwrap();
    let mut header = Vec::new();
    while !header.ends_with(b"\r\n\r\n") {
        header.push(socket.read_u8().await.unwrap());
    }
    let header = String::from_utf8(header).unwrap().to_ascii_lowercase();
    assert!(header.starts_with("http/1.1 101"), "{header}");
    assert!(header.contains("sec-websocket-protocol: graphql-transport-ws"));
    send(&mut socket, r#"{"type":"connection_init"}"#).await;
    assert!(String::from_utf8(recv(&mut socket).await.1)
        .unwrap()
        .contains("connection_ack"));
    send(
        &mut socket,
        r#"{"type":"subscribe","id":"idle","payload":{"query":"idle"}}"#,
    )
    .await;
    send(
        &mut socket,
        r#"{"type":"subscribe","id":"live","payload":{"query":"live"}}"#,
    )
    .await;
    let (_, next) = tokio::time::timeout(Duration::from_secs(1), recv(&mut socket))
        .await
        .unwrap();
    let next: serde_json::Value = serde_json::from_slice(&next).unwrap();
    assert_eq!(next["type"], "next");
    assert_eq!(next["id"], "live");
    let complete: serde_json::Value = serde_json::from_slice(&recv(&mut socket).await.1).unwrap();
    assert_eq!(complete["type"], "complete");
    send(&mut socket, r#"{"type":"complete","id":"idle"}"#).await;
    send(&mut socket, r#"{"type":"ping","payload":{"test":true}}"#).await;
    let pong: serde_json::Value = serde_json::from_slice(&recv(&mut socket).await.1).unwrap();
    assert_eq!(pong["type"], "pong");
    assert_eq!(pong["payload"]["test"], true);
    assert!(dropped.load(Ordering::SeqCst));
    send(&mut socket, r#"{"type":"connection_init"}"#).await;
    let (opcode, close) = recv(&mut socket).await;
    assert_eq!(opcode, 8);
    assert_eq!(u16::from_be_bytes([close[0], close[1]]), 4429);
    drop(socket);
    stop.send(()).unwrap();
    server.await.unwrap();
}
