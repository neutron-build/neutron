//! Typed RPC layer built on top of [`neutron-grpc`].
//!
//! Provides a zero-proto-file RPC framework: define your request and response
//! types as plain Rust structs (implementing `serde::Serialize + Deserialize`),
//! register handlers on an [`RpcServer`] builder, and mount the resulting
//! [`GrpcRouter`] in your application.
//!
//! ## Wire format
//!
//! Messages are JSON-encoded inside the standard gRPC 5-byte length-prefix
//! envelope.  Any gRPC client can call these services if it sends and expects
//! JSON bodies.
//!
//! ## Quick start
//!
//! ```rust,ignore
//! use serde::{Deserialize, Serialize};
//! use neutron_rpc::{RpcError, RpcServer};
//! use neutron::prelude::*;
//!
//! #[derive(Serialize, Deserialize)]
//! struct HelloRequest  { name: String }
//!
//! #[derive(Serialize, Deserialize)]
//! struct HelloReply    { message: String }
//!
//! async fn say_hello(req: HelloRequest) -> Result<HelloReply, RpcError> {
//!     Ok(HelloReply { message: format!("Hello, {}!", req.name) })
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let grpc = RpcServer::new("helloworld.Greeter")
//!         .method("SayHello", say_hello)
//!         .into_grpc_router();
//!
//!     let router = Router::new()
//!         .get("/healthz", || async { "ok" })
//!         .nest("/", grpc.into_router());
//!
//!     Neutron::new().router(router).listen("0.0.0.0:3000".parse().unwrap()).await.unwrap();
//! }
//! ```

pub mod codec;
pub mod error;
pub mod server;

#[cfg(feature = "protobuf")]
pub mod proto_codec;

pub use codec::{CodecError, RpcMessage};
pub use error::RpcError;
pub use server::RpcServer;

// Re-export the underlying gRPC types for convenience.
pub use neutron_grpc::{GrpcRouter, GrpcStatus};

// Re-export Proto<T> when the protobuf feature is enabled.
#[cfg(feature = "protobuf")]
pub use proto_codec::Proto;
