//! gRPC addon for Neutron.
//!
//! Provides the wire-framing, status codes, extractors, and response types
//! needed to write gRPC services using Neutron's handler model. Works with
//! any protobuf codec — decode/encode inside your handler.
//!
//! ```rust,ignore
//! async fn say_hello(
//!     State(db): State<Db>,
//!     GrpcRequest(payload): GrpcRequest,
//! ) -> GrpcResponse {
//!     let req = HelloRequest::decode(payload.as_ref()).unwrap();
//!     let reply = HelloReply { message: format!("Hello, {}!", req.name) };
//!     GrpcResponse::ok(reply.encode_to_vec())
//! }
//!
//! let router = GrpcRouter::new()
//!     .method("/helloworld.Greeter/SayHello", say_hello)
//!     .state(db)
//!     .into_router();
//! ```

pub mod body;
pub mod request;
pub mod response;
pub mod router;
pub mod status;

pub use request::GrpcRequest;
pub use response::GrpcResponse;
pub use router::GrpcRouter;
pub use status::{GrpcError, GrpcStatus};

pub mod prelude {
    pub use crate::{GrpcError, GrpcRequest, GrpcResponse, GrpcRouter, GrpcStatus};
}
