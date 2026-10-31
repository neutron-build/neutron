//! RS-29 regression: the published `graphql_handler` factory must satisfy
//! the Router's `Handler` bounds when mounted exactly as documented — the
//! opaque return type previously lacked `Clone`, so this file did not
//! compile. Same for `Fn(Request)` factories (see the oauth crate's test).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use neutron::prelude::*;
use neutron_graphql::{ExecutableSchema, GraphQlRequest, GraphQlResponse};

struct PingSchema;

impl ExecutableSchema for PingSchema {
    fn execute(
        self: Arc<Self>,
        _req: GraphQlRequest,
    ) -> Pin<Box<dyn Future<Output = GraphQlResponse> + Send + 'static>> {
        Box::pin(async { GraphQlResponse::ok(serde_json::json!({"data": {"ping": true}})) })
    }
}

#[tokio::test]
async fn graphql_handler_mounts_on_router() {
    let router = Router::new()
        .get("/graphql", neutron_graphql::graphql_handler(PingSchema))
        .post("/graphql", neutron_graphql::graphql_handler(PingSchema));

    // The compile itself is the regression (Handler bounds). Exercise the
    // dispatch once through the testing feature's client.
    let client = neutron::testing::TestClient::new(router);
    let resp = client.get("/graphql?query=%7Bping%7D").send().await;
    assert_eq!(resp.status(), 200);
    let body = resp.text().await;
    assert!(body.contains("ping"), "body: {body}");
}
