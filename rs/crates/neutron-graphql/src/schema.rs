//! `ExecutableSchema` trait — plug any GraphQL engine into Neutron.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::request::GraphQlRequest;
use crate::response::GraphQlResponse;

/// Trait for types that can execute a GraphQL request.
///
/// Implement this to bridge any schema engine (async-graphql, juniper, etc.)
/// into Neutron's handler system.
///
/// The method takes `Arc<Self>` so implementors can clone cheaply — the `Arc`
/// is provided by `graphql_handler` and costs only a reference-count increment
/// per request.
///
/// ```rust,ignore
/// use neutron_graphql::{ExecutableSchema, GraphQlRequest, GraphQlResponse};
///
/// struct MySchema {
///     inner: async_graphql::Schema<Query, Mutation, Subscription>,
/// }
///
/// impl ExecutableSchema for MySchema {
///     fn execute(
///         self: Arc<Self>,
///         req: GraphQlRequest,
///     ) -> Pin<Box<dyn Future<Output = GraphQlResponse> + Send + 'static>> {
///         Box::pin(async move {
///             let result = self.inner
///                 .execute(req.query)
///                 .variables(req.variables.unwrap_or_default())
///                 .await;
///             GraphQlResponse::ok(result.data.into_json().unwrap())
///         })
///     }
/// }
/// ```
pub trait ExecutableSchema: Send + Sync + 'static {
    fn execute(
        self: Arc<Self>,
        req: GraphQlRequest,
    ) -> Pin<Box<dyn Future<Output = GraphQlResponse> + Send + 'static>>;
}
