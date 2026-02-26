//! Handler factory — bridges `ExecutableSchema` into a Neutron route handler.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::request::GraphQlRequest;
use crate::response::GraphQlResponse;
use crate::schema::ExecutableSchema;

/// Create a Neutron handler for a GraphQL schema.
///
/// The returned handler parses the incoming request (GET or POST), passes it to
/// `schema.execute()`, and serializes the result as JSON. Register it on both
/// GET and POST to support all clients:
///
/// ```rust,ignore
/// let schema = Arc::new(MySchema::new());
///
/// let router = Router::new()
///     .get("/graphql",  graphql_handler(schema.clone()))
///     .post("/graphql", graphql_handler(schema));
/// ```
///
/// Because the closure is `Fn` (not `FnOnce`), the schema is shared across
/// all concurrent requests via reference counting — no mutex, no contention.
pub fn graphql_handler<S: ExecutableSchema>(
    schema: S,
) -> impl Fn(GraphQlRequest) -> Pin<Box<dyn Future<Output = GraphQlResponse> + Send + 'static>>
       + Send
       + Sync
       + 'static
{
    let schema = Arc::new(schema);
    move |req: GraphQlRequest| {
        let schema = Arc::clone(&schema);
        schema.execute(req)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http::{HeaderMap, Method};
    use neutron::extract::FromRequest;
    use neutron::handler::{IntoResponse, Request};
    use http_body_util::BodyExt;

    struct EchoSchema;

    impl ExecutableSchema for EchoSchema {
        fn execute(
            self: Arc<Self>,
            req: GraphQlRequest,
        ) -> Pin<Box<dyn Future<Output = GraphQlResponse> + Send + 'static>> {
            Box::pin(async move {
                GraphQlResponse::ok(serde_json::json!({ "query": req.query }))
            })
        }
    }

    fn post_json_req(body: &str) -> Request {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/json".parse().unwrap());
        Request::new(
            Method::POST,
            "/graphql".parse().unwrap(),
            headers,
            Bytes::copy_from_slice(body.as_bytes()),
        )
    }

    fn ok_or_panic<T>(r: Result<T, neutron::handler::Response>, msg: &str) -> T {
        match r { Ok(v) => v, Err(resp) => panic!("{msg}: HTTP {}", resp.status()) }
    }

    #[tokio::test]
    async fn handler_executes_schema_and_returns_json() {
        let handler = graphql_handler(EchoSchema);

        let req = ok_or_panic(
            GraphQlRequest::from_request(&post_json_req(r#"{"query":"{ users { id } }"}"#)),
            "GraphQL parse failed",
        );

        let resp = handler(req).await.into_response();
        assert_eq!(resp.status(), http::StatusCode::OK);

        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["data"]["query"], "{ users { id } }");
    }

    #[tokio::test]
    async fn handler_can_be_called_multiple_times() {
        let handler = graphql_handler(EchoSchema);

        for query in ["q1", "q2", "q3"] {
            let req = GraphQlRequest {
                query: query.to_string(),
                variables: None,
                operation_name: None,
            };
            let resp = handler(req).await;
            assert_eq!(resp.data.as_ref().unwrap()["query"], query);
        }
    }

    #[tokio::test]
    async fn handler_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>(_: &T) {}
        let handler = graphql_handler(EchoSchema);
        assert_send_sync(&handler);
    }
}
