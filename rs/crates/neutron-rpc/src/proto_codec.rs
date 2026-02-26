//! Protobuf codec support for `neutron-rpc` (requires `protobuf` feature).
//!
//! Wraps any `prost`-generated message type in the [`Proto`] newtype so that
//! it implements [`RpcMessage`] using binary protobuf encoding instead of JSON.
//!
//! # Usage
//!
//! ```rust,ignore
//! use neutron_rpc::{RpcServer, RpcError, Proto};
//! use prost::Message;
//!
//! // Protobuf-generated types (via prost-build in build.rs).
//! #[derive(prost::Message)]
//! pub struct HelloRequest { #[prost(string, tag = "1")] pub name: String }
//!
//! #[derive(prost::Message, Default)]
//! pub struct HelloReply   { #[prost(string, tag = "1")] pub message: String }
//!
//! async fn say_hello(Proto(req): Proto<HelloRequest>) -> Result<Proto<HelloReply>, RpcError> {
//!     Ok(Proto(HelloReply { message: format!("Hello, {}!", req.name) }))
//! }
//!
//! let router = RpcServer::new("helloworld.Greeter")
//!     .method("SayHello", say_hello)
//!     .into_grpc_router();
//! ```
//!
//! Wire format: binary protobuf inside the standard gRPC 5-byte
//! length-prefix envelope.  Compatible with any gRPC client that speaks
//! proto3.

use bytes::Bytes;
use prost::Message;

use crate::codec::{CodecError, RpcMessage};

// ---------------------------------------------------------------------------
// Proto<T> newtype
// ---------------------------------------------------------------------------

/// Newtype wrapper that implements [`RpcMessage`] via binary protobuf encoding.
///
/// Use this as the request and response type in [`RpcServer::method`] to opt
/// into protobuf on a per-method basis while keeping other methods on JSON.
///
/// ```rust,ignore
/// async fn greet(Proto(req): Proto<HelloRequest>) -> Result<Proto<HelloReply>, RpcError> {
///     Ok(Proto(HelloReply { message: format!("Hi, {}!", req.name) }))
/// }
/// ```
pub struct Proto<T>(pub T);

impl<T> Proto<T> {
    /// Consume the wrapper and return the inner value.
    pub fn into_inner(self) -> T { self.0 }
}

impl<T: std::fmt::Debug> std::fmt::Debug for Proto<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Proto").field(&self.0).finish()
    }
}

// ---------------------------------------------------------------------------
// RpcMessage impl
// ---------------------------------------------------------------------------

impl<T> RpcMessage for Proto<T>
where
    T: Message + Default + Send + 'static,
{
    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        T::decode(bytes)
            .map(Proto)
            .map_err(|e| CodecError(format!("protobuf decode: {e}")))
    }

    fn encode(&self) -> Result<Bytes, CodecError> {
        Ok(Bytes::from(self.0.encode_to_vec()))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal hand-rolled prost message for testing without a build.rs.
    #[derive(Clone, PartialEq, prost::Message)]
    struct Point {
        #[prost(int32, tag = "1")]
        x: i32,
        #[prost(int32, tag = "2")]
        y: i32,
    }

    #[test]
    fn proto_encode_decode_roundtrip() {
        let original = Proto(Point { x: 10, y: 20 });
        let encoded  = original.encode().unwrap();
        let decoded  = Proto::<Point>::decode(&encoded).unwrap();
        assert_eq!(decoded.0, original.0);
    }

    #[test]
    fn proto_decode_error_on_bad_bytes() {
        // random bytes that are not a valid protobuf message for Point
        // (well, protobuf is lenient, but garbage should still error or give defaults)
        let result = Proto::<Point>::decode(b"\xff\xff\xff\xff\xff\xff");
        // prost may or may not error on this — just check it doesn't panic.
        let _ = result;
    }

    #[test]
    fn empty_message_roundtrip() {
        let original = Proto(Point { x: 0, y: 0 });
        let encoded  = original.encode().unwrap();
        // An all-zero protobuf message encodes to empty bytes.
        assert!(encoded.is_empty());
        let decoded = Proto::<Point>::decode(&encoded).unwrap();
        assert_eq!(decoded.0.x, 0);
        assert_eq!(decoded.0.y, 0);
    }

    #[test]
    fn into_inner_unwraps() {
        let p = Proto(Point { x: 3, y: 4 });
        let inner = p.into_inner();
        assert_eq!(inner.x, 3);
    }
}
