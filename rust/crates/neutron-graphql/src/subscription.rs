//! GraphQL subscription transport over WebSocket using the `graphql-ws` protocol.
//!
//! Implements the [graphql-ws protocol](https://github.com/enisdenjo/graphql-ws/blob/master/PROTOCOL.md)
//! (the modern protocol used by Apollo Client 3+, urql, and others).
//!
//! # Protocol overview
//!
//! 1. Client upgrades to WebSocket — server sends `{"type":"connection_ack"}`
//! 2. Client sends `{"type":"subscribe","id":"1","payload":{"query":"subscription {...}"}}`
//! 3. Server sends `{"type":"next","id":"1","payload":{"data":{...}}}` per event
//! 4. Server sends `{"type":"complete","id":"1"}` when the stream ends
//! 5. Client sends `{"type":"complete","id":"1"}` to cancel
//!
//! # Usage
//!
//! ```rust,ignore
//! use neutron_graphql::{graphql_subscription_handler, SubscriptionSchema,
//!                       ExecutableSchema, GraphQlRequest, GraphQlResponse};
//! use std::{sync::Arc, pin::Pin};
//! use futures_util::Stream;
//!
//! struct MySchema;
//! impl ExecutableSchema for MySchema { /* ... */ }
//!
//! impl SubscriptionSchema for MySchema {
//!     fn subscribe(
//!         self: Arc<Self>,
//!         req: GraphQlRequest,
//!     ) -> Pin<Box<dyn Stream<Item = GraphQlResponse> + Send + 'static>> {
//!         Box::pin(async_stream::stream! {
//!             for i in 0u32..5 {
//!                 yield GraphQlResponse::ok(serde_json::json!({ "count": i }));
//!                 tokio::time::sleep(std::time::Duration::from_secs(1)).await;
//!             }
//!         })
//!     }
//! }
//!
//! let schema = Arc::new(MySchema);
//! let router = Router::new()
//!     .post("/graphql",    graphql_handler(schema.clone()))
//!     .get("/graphql/ws",  graphql_subscription_handler(schema));
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use futures_util::Stream;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;
use tokio_stream::StreamMap;

use crate::request::GraphQlRequest;
use crate::response::GraphQlResponse;
use neutron::extract::FromRequest;
use neutron::handler::{Request, Response};
use neutron::ws::{Message, WebSocket, WebSocketUpgrade};

// ---------------------------------------------------------------------------
// SubscriptionSchema trait
// ---------------------------------------------------------------------------

/// Extend [`ExecutableSchema`](crate::schema::ExecutableSchema) with
/// subscription support.
///
/// Implement alongside `ExecutableSchema` to enable the `graphql-ws`
/// WebSocket subscription protocol.
pub trait SubscriptionSchema: Send + Sync + 'static {
    /// Execute a subscription operation and return an event stream.
    ///
    /// Each yielded [`GraphQlResponse`] is delivered to the client as a
    /// `{"type":"next","id":"...","payload":{...}}` message.
    fn subscribe(
        self: Arc<Self>,
        req: GraphQlRequest,
    ) -> Pin<Box<dyn Stream<Item = GraphQlResponse> + Send + 'static>>;
}

// ---------------------------------------------------------------------------
// graphql-ws protocol messages
// ---------------------------------------------------------------------------

#[derive(Deserialize, Debug)]
struct ClientMessage {
    #[serde(rename = "type")]
    msg_type: String,
    id: Option<String>,
    payload: Option<Value>,
}

#[derive(Serialize)]
struct ServerMessage<'a> {
    #[serde(rename = "type")]
    msg_type: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    payload: Option<Value>,
}

fn make_ack() -> String {
    serde_json::to_string(&ServerMessage {
        msg_type: "connection_ack",
        id: None,
        payload: None,
    })
    .unwrap()
}

fn make_next(id: &str, payload: Value) -> String {
    serde_json::to_string(&ServerMessage {
        msg_type: "next",
        id: Some(id),
        payload: Some(payload),
    })
    .unwrap()
}

fn make_complete(id: &str) -> String {
    serde_json::to_string(&ServerMessage {
        msg_type: "complete",
        id: Some(id),
        payload: None,
    })
    .unwrap()
}

#[cfg(test)]
fn make_error(id: &str, message: &str) -> String {
    serde_json::to_string(&ServerMessage {
        msg_type: "error",
        id: Some(id),
        payload: Some(serde_json::json!([{"message": message}])),
    })
    .unwrap()
}

// ---------------------------------------------------------------------------
// Handler factory
// ---------------------------------------------------------------------------

/// Return a handler that upgrades HTTP connections to `graphql-ws` WebSocket.
///
/// Mount alongside your HTTP handler:
///
/// ```rust,ignore
/// let router = Router::new()
///     .post("/graphql",    graphql_handler(schema.clone()))
///     .get("/graphql/ws",  graphql_subscription_handler(schema));
/// ```
pub fn graphql_subscription_handler<S>(
    schema: Arc<S>,
) -> impl Fn(Request) -> Pin<Box<dyn Future<Output = Response> + Send>> + Clone + Send + Sync + 'static
where
    S: SubscriptionSchema,
{
    move |req: Request| {
        let schema = Arc::clone(&schema);
        Box::pin(async move {
            let mut req = req;
            if !req
                .headers()
                .get("sec-websocket-protocol")
                .and_then(|value| value.to_str().ok())
                .map(|value| {
                    value
                        .split(',')
                        .any(|protocol| protocol.trim() == "graphql-transport-ws")
                })
                .unwrap_or(false)
            {
                return http::Response::builder()
                    .status(400)
                    .body(neutron::handler::Body::full(
                        "graphql-transport-ws subprotocol required",
                    ))
                    .unwrap();
            }
            // Extract the WebSocket upgrade from the request.
            let ws = match WebSocketUpgrade::from_request(&mut req).await {
                Ok(ws) => ws,
                Err(err) => return err,
            };

            // Negotiate the graphql-ws subprotocol and begin the upgrade.
            ws.protocols(&["graphql-transport-ws"])
                .on_upgrade(move |socket| async move {
                    run_graphql_ws(socket, schema).await;
                })
        })
    }
}

// ---------------------------------------------------------------------------
// graphql-ws protocol runner
// ---------------------------------------------------------------------------

/// Drive the `graphql-ws` protocol on an established [`WebSocket`].
async fn run_graphql_ws<S: SubscriptionSchema>(socket: WebSocket, schema: Arc<S>) {
    let stop = socket.shutdown_signal();
    let (sender, receiver) = socket.split();
    tokio::pin!(stop);
    tokio::select! {
        _ = &mut stop => { close_protocol(&sender, 1001, "Server shutdown").await; }
        _ = run_session(sender.clone(), receiver, schema) => {}
    }
}

type OperationStream = Pin<Box<dyn Stream<Item = Option<GraphQlResponse>> + Send>>;

async fn run_session<S: SubscriptionSchema>(
    sender: neutron::ws::WsSender,
    mut receiver: neutron::ws::WsReceiver,
    schema: Arc<S>,
) {
    let mut init_done = false;
    let deadline = tokio::time::sleep(Duration::from_secs(3));
    tokio::pin!(deadline);
    let mut operations: StreamMap<String, OperationStream> = StreamMap::new();
    loop {
        tokio::select! {
            _ = &mut deadline, if !init_done => {
                close_protocol(&sender, 4408, "Connection initialisation timeout").await;
                break;
            }
            event = operations.next(), if !operations.is_empty() => {
                if let Some((id, response)) = event {
                    let message = if let Some(response) = response {
                        let mut payload = serde_json::Map::new();
                        if let Some(data) = response.data { payload.insert("data".into(), data); }
                        if !response.errors.is_empty() { payload.insert("errors".into(), serde_json::to_value(response.errors).unwrap()); }
                        make_next(&id, Value::Object(payload))
                    } else {
                        operations.remove(&id);
                        make_complete(&id)
                    };
                    if sender.send(Message::Text(message)).await.is_err() { break; }
                }
            }
            msg = receiver.recv() => {
                let text = match msg {
                    Some(Message::Text(text)) => text,
                    Some(Message::Ping(data)) => { if sender.send(Message::Pong(data)).await.is_err() { break; } continue; }
                    Some(Message::Pong(_)) => continue,
                    _ => break,
                };
                let message: ClientMessage = match serde_json::from_str(&text) {
                    Ok(message) => message,
                    Err(_) => { close_protocol(&sender, 4400, "Invalid message").await; break; }
                };
                if !valid_client_shape(&message) { close_protocol(&sender, 4400, "Invalid message shape").await; break; }
                match message.msg_type.as_str() {
                    "connection_init" => {
                        if init_done { close_protocol(&sender, 4429, "Too many initialisation requests").await; break; }
                        init_done = true;
                        if sender.send(Message::Text(make_ack())).await.is_err() { break; }
                    }
                    "ping" => {
                        let pong = serde_json::to_string(&ServerMessage { msg_type: "pong", id: None, payload: message.payload }).unwrap();
                        if sender.send(Message::Text(pong)).await.is_err() { break; }
                    }
                    "pong" => {}
                    "subscribe" => {
                        if !init_done { close_protocol(&sender, 4401, "Unauthorised").await; break; }
                        let Some(id) = message.id.filter(|id| !id.is_empty()) else { close_protocol(&sender, 4400, "Missing operation ID").await; break; };
                        if operations.contains_key(&id) { close_protocol(&sender, 4409, "Subscriber already exists").await; break; }
                        match message.payload.ok_or_else(|| "missing payload".to_string()).and_then(parse_subscribe_payload) {
                            Ok(req) => {
                                let stream = schema.clone().subscribe(req).map(Some).chain(futures_util::stream::once(async { None }));
                                operations.insert(id, Box::pin(stream));
                            }
                            Err(_) => { close_protocol(&sender, 4400, "Invalid subscribe payload").await; break; }
                        }
                    }
                    "complete" => {
                        if let Some(id) = message.id { operations.remove(&id); }
                        else { close_protocol(&sender, 4400, "Missing operation ID").await; break; }
                    }
                    _ => { close_protocol(&sender, 4400, "Invalid message type").await; break; }
                }
            }
        }
    }
    // Dropping all streams cancels every active operation, including idle ones.
}

async fn close_protocol(sender: &neutron::ws::WsSender, code: u16, reason: &str) {
    let _ = sender
        .send(Message::Close(Some(neutron::ws::CloseFrame {
            code,
            reason: reason.into(),
        })))
        .await;
}

fn valid_client_shape(message: &ClientMessage) -> bool {
    match message.msg_type.as_str() {
        "connection_init" | "ping" | "pong" => {
            message.id.is_none()
                && message
                    .payload
                    .as_ref()
                    .is_none_or(|p| p.is_null() || p.is_object())
        }
        "subscribe" => {
            message.id.as_ref().is_some_and(|id| !id.is_empty())
                && message
                    .payload
                    .as_ref()
                    .is_some_and(|p| parse_subscribe_payload(p.clone()).is_ok())
        }
        "complete" => {
            message.id.as_ref().is_some_and(|id| !id.is_empty()) && message.payload.is_none()
        }
        _ => false,
    }
}

fn parse_subscribe_payload(payload: Value) -> Result<GraphQlRequest, String> {
    let query = payload
        .get("query")
        .and_then(Value::as_str)
        .ok_or("payload.query is required")?
        .to_string();

    if !payload.is_object()
        || payload
            .get("variables")
            .is_some_and(|v| !v.is_null() && !v.is_object())
        || payload
            .get("operationName")
            .is_some_and(|v| !v.is_null() && !v.is_string())
        || payload
            .get("extensions")
            .is_some_and(|v| !v.is_null() && !v.is_object())
    {
        return Err("invalid subscribe payload shape".into());
    }
    let operation_name = payload
        .get("operationName")
        .and_then(Value::as_str)
        .map(str::to_string);

    let variables = payload
        .get("variables")
        .and_then(Value::as_object)
        .map(|m| Value::Object(m.clone()));

    Ok(GraphQlRequest {
        query,
        variables,
        operation_name,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ack_message_type() {
        let msg: Value = serde_json::from_str(&make_ack()).unwrap();
        assert_eq!(msg["type"], "connection_ack");
        assert!(msg.get("id").is_none() || msg["id"].is_null());
    }

    #[test]
    fn next_message_contains_payload() {
        let payload = serde_json::json!({"data": {"count": 1}});
        let msg: Value = serde_json::from_str(&make_next("sub-1", payload)).unwrap();
        assert_eq!(msg["type"], "next");
        assert_eq!(msg["id"], "sub-1");
        assert_eq!(msg["payload"]["data"]["count"], 1);
    }

    #[test]
    fn complete_message_no_payload() {
        let msg: Value = serde_json::from_str(&make_complete("sub-1")).unwrap();
        assert_eq!(msg["type"], "complete");
        assert_eq!(msg["id"], "sub-1");
        assert!(msg.get("payload").map(|v| v.is_null()).unwrap_or(true));
    }

    #[test]
    fn error_message_has_errors_array() {
        let msg: Value = serde_json::from_str(&make_error("sub-1", "bad query")).unwrap();
        assert_eq!(msg["type"], "error");
        assert_eq!(msg["payload"][0]["message"], "bad query");
    }

    #[test]
    fn parse_subscribe_payload_full() {
        let payload = serde_json::json!({
            "query":         "subscription { count }",
            "operationName": "CountSub",
            "variables":     { "n": 5 }
        });
        let req = parse_subscribe_payload(payload).unwrap();
        assert_eq!(req.query, "subscription { count }");
        assert_eq!(req.operation_name.as_deref(), Some("CountSub"));
        assert_eq!(req.variables.as_ref().unwrap()["n"], 5);
    }

    #[test]
    fn parse_subscribe_payload_minimal() {
        let payload = serde_json::json!({ "query": "subscription { ping }" });
        let req = parse_subscribe_payload(payload).unwrap();
        assert_eq!(req.query, "subscription { ping }");
        assert!(req.operation_name.is_none());
        assert!(req.variables.is_none());
    }

    #[test]
    fn parse_subscribe_payload_missing_query() {
        let payload = serde_json::json!({ "variables": {} });
        assert!(parse_subscribe_payload(payload).is_err());
    }

    #[test]
    fn subscription_schema_trait_is_object_safe() {
        // Verify the trait can be used as a trait object.
        fn _accepts(_: Arc<dyn SubscriptionSchema>) {}
    }

    #[test]
    fn protocol_rejects_scalar_init_ping_and_mistyped_subscribe_fields() {
        for raw in [
            r#"{"type":"connection_init","payload":1}"#,
            r#"{"type":"ping","payload":true}"#,
            r#"{"type":"subscribe","id":"x","payload":{"query":"q","variables":[]}}"#,
            r#"{"type":"subscribe","id":"x","payload":{"query":"q","operationName":1}}"#,
        ] {
            let message: ClientMessage = serde_json::from_str(raw).unwrap();
            assert!(!valid_client_shape(&message));
        }
    }
}
