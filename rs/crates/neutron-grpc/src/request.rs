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
    fn from_request(req: &Request) -> Result<Self, Response> {
        // Validate Content-Type
        let ct = req
            .headers()
            .get(http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        if !ct.starts_with("application/grpc") {
            return Err(GrpcStatus::InvalidArgument
                .error_response("Expected Content-Type: application/grpc"));
        }

        let body = req.body();
        let (msg_bytes, _compressed) = unframe_message(body)
            .ok_or_else(|| GrpcStatus::InvalidArgument.error_response("malformed gRPC frame"))?;

        Ok(GrpcRequest(Bytes::copy_from_slice(msg_bytes)))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use crate::body::frame_message;
    use neutron::handler::Request;
    use http::{HeaderMap, Method};

    fn grpc_request(payload: &[u8]) -> Request {
        let framed = frame_message(Bytes::copy_from_slice(payload));
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/grpc".parse().unwrap());
        headers.insert("te", "trailers".parse().unwrap());
        Request::new(Method::POST, "/".parse().unwrap(), headers, framed)
    }

    fn ok_or_panic<T>(r: Result<T, Response>, msg: &str) -> T {
        match r { Ok(v) => v, Err(resp) => panic!("{msg}: HTTP {}", resp.status()) }
    }

    #[test]
    fn extracts_payload() {
        let req = grpc_request(b"hello grpc");
        let GrpcRequest(payload) = ok_or_panic(GrpcRequest::from_request(&req), "extract failed");
        assert_eq!(payload.as_ref(), b"hello grpc");
    }

    #[test]
    fn rejects_wrong_content_type() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/json".parse().unwrap());
        let req = Request::new(Method::POST, "/".parse().unwrap(), headers, Bytes::new());
        let result = GrpcRequest::from_request(&req);
        assert!(result.is_err());
    }

    #[test]
    fn rejects_malformed_frame() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/grpc".parse().unwrap());
        // Only 3 bytes — too short for the 5-byte header
        let req = Request::new(Method::POST, "/".parse().unwrap(), headers, Bytes::from_static(b"ab"));
        let result = GrpcRequest::from_request(&req);
        assert!(result.is_err());
    }

    #[test]
    fn extracts_empty_payload() {
        let req = grpc_request(b"");
        let GrpcRequest(payload) = ok_or_panic(GrpcRequest::from_request(&req), "empty extract failed");
        assert!(payload.is_empty());
    }
}
