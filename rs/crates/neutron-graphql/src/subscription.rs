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
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_stream::StreamExt;

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
    id:       Option<String>,
    payload:  Option<Value>,
}

#[derive(Serialize)]
struct ServerMessage<'a> {
    #[serde(rename = "type")]
    msg_type: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    id:       Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    payload:  Option<Value>,
}

fn make_ack() -> String {
    serde_json::to_string(&ServerMessage {
        msg_type: "connection_ack",
        id:       None,
        payload:  None,
    })
    .unwrap()
}

fn make_next(id: &str, payload: Value) -> String {
    serde_json::to_string(&ServerMessage {
        msg_type: "next",
        id:       Some(id),
        payload:  Some(payload),
    })
    .unwrap()
}

fn make_complete(id: &str) -> String {
    serde_json::to_string(&ServerMessage {
        msg_type: "complete",
        id:       Some(id),
        payload:  None,
    })
    .unwrap()
}

fn make_error(id: &str, message: &str) -> String {
    serde_json::to_string(&ServerMessage {
        msg_type: "error",
        id:       Some(id),
        payload:  Some(serde_json::json!([{"message": message}])),
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
) -> impl Fn(Request) -> Pin<Box<dyn Future<Output = Response> + Send>>
       + Clone
       + Send
       + Sync
       + 'static
where
    S: SubscriptionSchema,
{
    move |req: Request| {
        let schema = Arc::clone(&schema);
        Box::pin(async move {
            // Extract the WebSocket upgrade from the request.
            let ws = match WebSocketUpgrade::from_request(&req) {
                Ok(ws)   => ws,
                Err(err) => return err,
            };

            // Negotiate the graphql-ws subprotocol and begin the upgrade.
            ws.protocols(&["graphql-ws"])
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
async fn run_graphql_ws<S: SubscriptionSchema>(mut socket: WebSocket, schema: Arc<S>) {
    let mut init_done = false;

    loop {
        let msg = match socket.recv().await {
            Some(m) => m,
            None    => break, // Connection closed.
        };

        let text = match msg {
            Message::Text(t)  => t,
            Message::Close(_) => break,
            Message::Ping(d)  => {
                let _ = socket.send(Message::Pong(d)).await;
                continue;
            }
            _ => continue,
        };

        let client_msg: ClientMessage = match serde_json::from_str(&text) {
            Ok(m)  => m,
            Err(_) => break, // Malformed message — close.
        };

        match client_msg.msg_type.as_str() {
            // Step 1: client initialises the connection.
            "connection_init" => {
                init_done = true;
                if socket.send(Message::Text(make_ack())).await.is_err() {
                    break;
                }
            }

            // Step 2: client starts a subscription.
            "subscribe" if init_done => {
                let id = match client_msg.id {
                    Some(id) => id,
                    None     => continue,
                };
                let payload = match client_msg.payload {
                    Some(p) => p,
                    None    => {
                        let _ = socket.send(Message::Text(make_error(&id, "missing payload"))).await;
                        continue;
                    }
                };

                let gql_req = match parse_subscribe_payload(payload) {
                    Ok(r)  => r,
                    Err(e) => {
                        let _ = socket.send(Message::Text(make_error(&id, &e))).await;
                        continue;
                    }
                };

                // Stream events to the client.
                let mut stream = schema.clone().subscribe(gql_req);
                while let Some(resp) = stream.next().await {
                    // GraphQlResponse does not derive Serialize; build Value manually.
                    let mut map = serde_json::Map::new();
                    if let Some(data) = resp.data {
                        map.insert("data".to_string(), data);
                    }
                    if !resp.errors.is_empty() {
                        map.insert(
                            "errors".to_string(),
                            serde_json::to_value(&resp.errors).unwrap_or(Value::Array(vec![])),
                        );
                    }
                    let payload = Value::Object(map);
                    if socket.send(Message::Text(make_next(&id, payload))).await.is_err() {
                        return; // Connection dropped mid-stream.
                    }
                }

                // Stream ended normally — send complete.
                let _ = socket.send(Message::Text(make_complete(&id))).await;
            }

            // Client cancelled a subscription.
            "complete" => {
                // In a full multiplexed implementation you would cancel the
                // specific subscription by id.  For now we just acknowledge.
            }

            // Protocol violation before init.
            "subscribe" => {
                break;
            }

            // Unknown message type — ignore per spec.
            _ => {}
        }
    }
}

fn parse_subscribe_payload(payload: Value) -> Result<GraphQlRequest, String> {
    let query = payload
        .get("query")
        .and_then(Value::as_str)
        .ok_or("payload.query is required")?
        .to_string();

    let operation_name = payload
        .get("operationName")
        .and_then(Value::as_str)
        .map(str::to_string);

    let variables = payload
        .get("variables")
        .and_then(Value::as_object)
        .map(|m| Value::Object(m.clone()));

    Ok(GraphQlRequest { query, variables, operation_name })
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
        assert_eq!(msg["type"],                         "next");
        assert_eq!(msg["id"],                           "sub-1");
        assert_eq!(msg["payload"]["data"]["count"],     1);
    }

    #[test]
    fn complete_message_no_payload() {
        let msg: Value = serde_json::from_str(&make_complete("sub-1")).unwrap();
        assert_eq!(msg["type"], "complete");
        assert_eq!(msg["id"],   "sub-1");
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
        assert_eq!(req.query,                          "subscription { count }");
        assert_eq!(req.operation_name.as_deref(),      Some("CountSub"));
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
}
