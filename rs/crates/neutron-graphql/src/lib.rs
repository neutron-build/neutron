//! GraphQL HTTP transport addon for Neutron.
//!
//! Provides `GraphQlRequest` (GET + POST parsing), `GraphQlResponse`
//! (JSON `{data, errors}` envelope), an `ExecutableSchema` trait, and
//! `graphql_handler` — a factory that bridges any schema to a Neutron handler.
//!
//! ```rust,ignore
//! struct MySchema;
//!
//! impl ExecutableSchema for MySchema {
//!     fn execute(self: Arc<Self>, req: GraphQlRequest)
//!         -> Pin<Box<dyn Future<Output = GraphQlResponse> + Send + 'static>>
//!     {
//!         Box::pin(async move {
//!             // call your schema engine here
//!             GraphQlResponse::ok(serde_json::json!({ "hello": "world" }))
//!         })
//!     }
//! }
//!
//! let router = Router::new()
//!     .get("/graphql",  graphql_handler(schema.clone()))
//!     .post("/graphql", graphql_handler(schema));
//! ```

pub mod handler;
pub mod request;
pub mod response;
pub mod schema;
pub mod subscription;

pub use handler::graphql_handler;
pub use request::GraphQlRequest;
pub use response::{GraphQlError, GraphQlResponse};
pub use schema::ExecutableSchema;
pub use subscription::{graphql_subscription_handler, SubscriptionSchema};

pub mod prelude {
    pub use crate::{
        graphql_handler, graphql_subscription_handler,
        ExecutableSchema, GraphQlError, GraphQlRequest, GraphQlResponse,
        SubscriptionSchema,
    };
}
