//! Real-time features example: SSE streaming + WebSocket echo + PubSub chat.
//!
//! Run: `cargo run --example realtime`
//!
//! Endpoints:
//!   GET /events      — SSE stream (ticks every second)
//!   GET /ws          — WebSocket echo server
//!   POST /broadcast  — publish a message to all SSE listeners
//!   GET /health      — health check

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use neutron::prelude::*;
use neutron::ws::{Message, WebSocket, WebSocketUpgrade};
use serde::Deserialize;

// ---------------------------------------------------------------------------
// SSE: server-sent events with PubSub
// ---------------------------------------------------------------------------

async fn sse_events(State(ps): State<PubSub>) -> Sse {
    let mut rx = ps.subscribe::<String>("events");

    // Stream events from the PubSub topic
    Sse::new(async_stream::stream! {
        // Send an initial welcome event
        yield SseEvent::new().event("connected").data("SSE stream started");

        // Relay messages from PubSub
        while let Ok(msg) = rx.recv().await {
            yield SseEvent::new().event("message").data(&msg);
        }
    })
}

async fn broadcast(
    State(ps): State<PubSub>,
    Json(body): Json<BroadcastRequest>,
) -> (StatusCode, &'static str) {
    let count = ps.publish("events", &body.message);
    tracing::info!(listeners = count, msg = %body.message, "broadcast sent");
    (StatusCode::OK, "sent")
}

#[derive(Deserialize)]
struct BroadcastRequest {
    message: String,
}

// ---------------------------------------------------------------------------
// Background ticker: publishes a heartbeat every 2 seconds
// ---------------------------------------------------------------------------

fn start_ticker(ps: PubSub) {
    let counter = Arc::new(AtomicU64::new(0));
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(2)).await;
            let n = counter.fetch_add(1, Ordering::Relaxed);
            ps.publish("events", &format!("tick #{n}"));
        }
    });
}

// ---------------------------------------------------------------------------
// WebSocket: echo server with connection counting
// ---------------------------------------------------------------------------

async fn ws_handler(ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(handle_ws)
}

async fn handle_ws(mut socket: WebSocket) {
    tracing::info!("WebSocket client connected");

    // Send a welcome message
    let _ = socket
        .send(Message::text("Welcome to the Neutron WebSocket echo server!"))
        .await;

    while let Some(msg) = socket.recv().await {
        match msg {
            Message::Text(text) => {
                tracing::info!(msg = %text, "WS received");
                let _ = socket
                    .send(Message::text(format!("Echo: {text}")))
                    .await;
            }
            Message::Binary(data) => {
                let _ = socket.send(Message::binary(data)).await;
            }
            Message::Ping(data) => {
                let _ = socket.send(Message::Pong(data)).await;
            }
            Message::Close(_) => {
                tracing::info!("WebSocket client disconnected");
                break;
            }
            _ => {}
        }
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    let pubsub = PubSub::new();

    // Start background heartbeat
    start_ticker(pubsub.clone());

    let router = Router::new()
        .state(pubsub)
        .middleware(Logger)
        .middleware(RequestId::new())
        .middleware(Cors::new().allow_any_origin().allow_any_method().allow_any_header())
        .get("/events", sse_events)
        .post("/broadcast", broadcast)
        .get("/ws", ws_handler)
        .get("/health", || async { Json(serde_json::json!({ "status": "ok" })) })
        .get("/", || async {
            (
                StatusCode::OK,
                "Neutron Realtime Example\n\n\
                 GET  /events     - SSE stream\n\
                 POST /broadcast  - send message to SSE listeners\n\
                 GET  /ws         - WebSocket echo\n\
                 GET  /health     - health check\n",
            )
        });

    let addr = "0.0.0.0:3001".parse()?;
    Neutron::new().router(router).listen(addr).await
}
