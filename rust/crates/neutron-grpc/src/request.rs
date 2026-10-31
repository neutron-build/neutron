//! gRPC request extractor.

use bytes::Bytes;
use neutron::extract::FromRequest;
use neutron::handler::{Request, Response};

use crate::body::unframe_message;
use crate::status::GrpcStatus;

/// Extract raw gRPC message bytes from the request body (5-byte frame stripped).
///
/// The inner `Bytes` contains the raw message — decode with any codec:
///
/// ```rust,ignore
/// async fn say_hello(GrpcRequest(payload): GrpcRequest) -> GrpcResponse {
///     let req = HelloRequest::decode(payload.as_ref()).unwrap();
///     let reply = HelloReply { message: format!("Hello, {}!", req.name) };
///     GrpcResponse::ok(reply.encode_to_vec())
/// }
/// ```
pub struct GrpcRequest(pub Bytes);

impl FromRequest for GrpcRequest {
    async fn from_request(req: &mut Request) -> Result<Self, Response> {
        // Validate Content-Type
        let ct = req
            .headers()
            .get(http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_owned();

        if !ct.starts_with("application/grpc") {
            return Err(GrpcStatus::InvalidArgument
                .error_response("Expected Content-Type: application/grpc"));
        }

        // gRPC messages are length-prefixed and bounded; buffer with a 4 MiB cap.
        let body = req
            .collect_body(4 * 1024 * 1024)
            .await
            .map_err(|_| GrpcStatus::ResourceExhausted.error_response("gRPC body too large"))?;
        let (msg_bytes, compressed) = unframe_message(&body)
            .ok_or_else(|| GrpcStatus::InvalidArgument.error_response("malformed gRPC frame"))?;

        // Unary extraction takes exactly one uncompressed message frame.
        // A compressed frame would hand still-compressed bytes to the
        // application as if they were the message; this crate implements no
        // grpc-encoding decompression, so refuse (fail closed) rather than
        // parse garbage.
        if compressed {
            return Err(GrpcStatus::Unimplemented
                .error_response("compressed gRPC messages are not supported"));
        }
        // Trailing bytes after the first frame mean more frames (or junk):
        // silently ignoring them dropped parts of the request.
        if body.len() > 5 + msg_bytes.len() {
            return Err(GrpcStatus::InvalidArgument
                .error_response("multiple gRPC frames are not valid on a unary request"));
        }

        Ok(GrpcRequest(Bytes::copy_from_slice(msg_bytes)))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::body::frame_message;
    use bytes::Bytes;
    use http::{HeaderMap, Method};
    use neutron::handler::Request;

    fn grpc_request(payload: &[u8]) -> Request {
        let framed = frame_message(Bytes::copy_from_slice(payload));
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/grpc".parse().unwrap());
        headers.insert("te", "trailers".parse().unwrap());
        Request::new(Method::POST, "/".parse().unwrap(), headers, framed)
    }

    fn ok_or_panic<T>(r: Result<T, Response>, msg: &str) -> T {
        match r {
            Ok(v) => v,
            Err(resp) => panic!("{msg}: HTTP {}", resp.status()),
        }
    }

    #[tokio::test]
    async fn extracts_payload() {
        let mut req = grpc_request(b"hello grpc");
        let GrpcRequest(payload) =
            ok_or_panic(GrpcRequest::from_request(&mut req).await, "extract failed");
        assert_eq!(payload.as_ref(), b"hello grpc");
    }

    /// Regression (RS-34): a unary request with a single empty frame is a
    /// valid (empty) message and must extract successfully.
    #[tokio::test]
    async fn extracts_empty_message() {
        let mut req = grpc_request(b"");
        let GrpcRequest(payload) = ok_or_panic(
            GrpcRequest::from_request(&mut req).await,
            "empty message rejected",
        );
        assert!(payload.is_empty());
    }

    /// Regression (RS-34): compressed frames must be refused (fail closed),
    /// not handed to the application as if the compressed bytes were the
    /// message.
    #[tokio::test]
    async fn rejects_compressed_frame() {
        let mut framed = vec![1u8]; // compressed flag
        framed.extend_from_slice(&4u32.to_be_bytes());
        framed.extend_from_slice(b"data");
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/grpc".parse().unwrap());
        let mut req = Request::new(
            Method::POST,
            "/".parse().unwrap(),
            headers,
            Bytes::from(framed),
        );
        assert!(GrpcRequest::from_request(&mut req).await.is_err());
    }

    /// Regression (RS-34): trailing frames after the first were silently
    /// dropped; a unary request carrying two frames must be rejected.
    #[tokio::test]
    async fn rejects_trailing_frames() {
        let mut framed = frame_message(Bytes::from_static(b"first")).to_vec();
        framed.extend_from_slice(&frame_message(Bytes::from_static(b"second")));
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/grpc".parse().unwrap());
        let mut req = Request::new(
            Method::POST,
            "/".parse().unwrap(),
            headers,
            Bytes::from(framed),
        );
        assert!(GrpcRequest::from_request(&mut req).await.is_err());
    }

    #[tokio::test]
    async fn rejects_wrong_content_type() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/json".parse().unwrap());
        let mut req = Request::new(Method::POST, "/".parse().unwrap(), headers, Bytes::new());
        let result = GrpcRequest::from_request(&mut req).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn rejects_malformed_frame() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/grpc".parse().unwrap());
        // Only 3 bytes — too short for the 5-byte header
        let mut req = Request::new(
            Method::POST,
            "/".parse().unwrap(),
            headers,
            Bytes::from_static(b"ab"),
        );
        let result = GrpcRequest::from_request(&mut req).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn extracts_empty_payload() {
        let mut req = grpc_request(b"");
        let GrpcRequest(payload) = ok_or_panic(
            GrpcRequest::from_request(&mut req).await,
            "empty extract failed",
        );
        assert!(payload.is_empty());
    }
}
