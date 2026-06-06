//! `RpcServer` builder — registers typed RPC methods and produces a `GrpcRouter`.

use std::future::Future;

use neutron_grpc::{GrpcRequest, GrpcResponse, GrpcRouter};
use neutron::handler::Handler;

use crate::codec::RpcMessage;
use crate::error::RpcError;

// ---------------------------------------------------------------------------
// RpcServer
// ---------------------------------------------------------------------------

/// Typed RPC service builder.
///
/// Methods are registered with [`method`](RpcServer::method).  Once all
/// methods are registered call [`into_grpc_router`](RpcServer::into_grpc_router)
/// to obtain a [`GrpcRouter`] for mounting in the main application.
///
/// **No `.proto` files required.** Any type that implements `serde::Serialize +
/// serde::de::DeserializeOwned` works as a request or response — the wire
/// format is JSON inside the gRPC length-prefix envelope.
///
/// ```rust,ignore
/// use serde::{Deserialize, Serialize};
/// use neutron_rpc::{RpcServer, RpcError};
///
/// #[derive(Serialize, Deserialize)]
/// struct HelloRequest { name: String }
///
/// #[derive(Serialize, Deserialize)]
/// struct HelloReply { message: String }
///
/// async fn say_hello(req: HelloRequest) -> Result<HelloReply, RpcError> {
///     Ok(HelloReply { message: format!("Hello, {}!", req.name) })
/// }
///
/// let router = RpcServer::new("helloworld.Greeter")
///     .method("SayHello", say_hello)
///     .into_grpc_router();
/// ```
pub struct RpcServer {
    /// Service package + name, e.g. `"helloworld.Greeter"`.
    service: String,
    router:  GrpcRouter,
}

impl RpcServer {
    /// Create a new service builder.
    ///
    /// `service` is the dotted package + service name used to form the route
    /// path: `/{service}/{Method}`.
    pub fn new(service: impl Into<String>) -> Self {
        Self { service: service.into(), router: GrpcRouter::new() }
    }

    /// Register a typed RPC method.
    ///
    /// The handler receives a decoded `Req` and must return
    /// `Result<Resp, RpcError>`.  Encoding / decoding uses [`RpcMessage`]
    /// (blanket-implemented for all serde types via JSON).
    pub fn method<Req, Resp, F, Fut>(self, name: &str, handler: F) -> Self
    where
        Req:  RpcMessage,
        Resp: RpcMessage,
        F:    Fn(Req) -> Fut + Send + Sync + Clone + 'static,
        Fut:  Future<Output = Result<Resp, RpcError>> + Send + 'static,
    {
        let path    = format!("/{}/{}", self.service, name);
        let handler = wrap_handler(handler);

        Self {
            service: self.service,
            router:  self.router.method(&path, handler),
        }
    }

    /// Register shared state accessible via `State<T>` in RPC handlers.
    pub fn state<T: Send + Sync + 'static>(self, value: T) -> Self {
        Self { service: self.service, router: self.router.state(value) }
    }

    /// Consume the builder and produce a [`GrpcRouter`].
    pub fn into_grpc_router(self) -> GrpcRouter {
        self.router
    }
}

// ---------------------------------------------------------------------------
// Internal: lift typed (Req -> Resp) handler into a GrpcRequest handler
// ---------------------------------------------------------------------------

fn wrap_handler<Req, Resp, F, Fut>(
    handler: F,
) -> impl Handler<(GrpcRequest,)> + Clone + 'static
where
    Req:  RpcMessage,
    Resp: RpcMessage,
    F:    Fn(Req) -> Fut + Send + Sync + Clone + 'static,
    Fut:  Future<Output = Result<Resp, RpcError>> + Send + 'static,
{
    move |GrpcRequest(bytes): GrpcRequest| {
        let handler = handler.clone();
        async move {
            // 1. Decode request.
            let req = match Req::decode(&bytes) {
                Ok(r)  => r,
                Err(e) => {
                    return GrpcResponse::error(
                        neutron_grpc::GrpcStatus::InvalidArgument,
                        format!("decode error: {}", e.0),
                    );
                }
            };

            // 2. Call handler.
            match handler(req).await {
                Ok(resp) => {
                    // 3. Encode response.
                    match resp.encode() {
                        Ok(bytes)  => GrpcResponse::ok(bytes),
                        Err(e) => GrpcResponse::error(
                            neutron_grpc::GrpcStatus::Internal,
                            format!("encode error: {}", e.0),
                        ),
                    }
                }
                Err(e) => GrpcResponse::error(e.status, e.message),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug)]
    struct Req  { name: String }
    #[derive(Serialize, Deserialize, Debug)]
    struct Resp { greeting: String }

    async fn greet(req: Req) -> Result<Resp, RpcError> {
        Ok(Resp { greeting: format!("Hello, {}!", req.name) })
    }

    async fn always_fails(_req: Req) -> Result<Resp, RpcError> {
        Err(RpcError::not_found("no such user"))
    }

    #[test]
    fn server_builds_grpc_router() {
        let _router = RpcServer::new("test.TestSvc")
            .method("Greet", greet)
            .into_grpc_router();
    }

    #[test]
    fn state_flows_through() {
        let _router = RpcServer::new("test.TestSvc")
            .method("Greet", greet)
            .state(42u64)
            .into_grpc_router();
    }

    fn ok_or_panic<T>(r: Result<T, neutron::handler::Response>, msg: &str) -> T {
        match r { Ok(v) => v, Err(resp) => panic!("{msg}: HTTP {}", resp.status()) }
    }

    #[tokio::test]
    async fn wrapped_handler_ok_path() {
        use neutron_grpc::body::frame_message;
        use neutron::handler::{IntoResponse, Request};
        use neutron::extract::FromRequest;
        use http::{HeaderMap, Method};
        use bytes::Bytes;
        use http_body_util::BodyExt;

        // Build a synthetic gRPC HTTP request.
        let req_body = serde_json::to_vec(&Req { name: "world".to_string() }).unwrap();
        let framed   = frame_message(Bytes::from(req_body));
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/grpc".parse().unwrap());
        let http_req = Request::new(Method::POST, "/".parse().unwrap(), headers, framed);

        // Extract GrpcRequest manually and simulate the wrap_handler pipeline.
        let GrpcRequest(bytes) = ok_or_panic(GrpcRequest::from_request(&http_req), "extract");

        let req: Req  = Req::decode(&bytes).unwrap();
        let resp: Resp = greet(req).await.unwrap();
        let encoded    = resp.encode().unwrap();
        let grpc_resp  = GrpcResponse::ok(encoded);

        // Verify the response has grpc-status: 0 trailer.
        let http_resp = grpc_resp.into_response();
        let collected = http_resp.into_body().collect().await.unwrap();
        let trailers  = collected.trailers().cloned().unwrap_or_default();
        assert_eq!(trailers.get("grpc-status").unwrap(), "0");
    }

    #[tokio::test]
    async fn wrapped_handler_error_path() {
        use neutron_grpc::body::frame_message;
        use neutron::handler::{IntoResponse, Request};
        use neutron::extract::FromRequest;
        use http::{HeaderMap, Method};
        use bytes::Bytes;
        use http_body_util::BodyExt;

        let req_body = serde_json::to_vec(&Req { name: "nobody".to_string() }).unwrap();
        let framed   = frame_message(Bytes::from(req_body));
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/grpc".parse().unwrap());
        let http_req = Request::new(Method::POST, "/".parse().unwrap(), headers, framed);

        let GrpcRequest(bytes) = ok_or_panic(GrpcRequest::from_request(&http_req), "extract");
        let req: Req    = Req::decode(&bytes).unwrap();
        let rpc_err     = always_fails(req).await.unwrap_err();
        let grpc_resp   = GrpcResponse::error(rpc_err.status, rpc_err.message);

        let http_resp = grpc_resp.into_response();
        let collected = http_resp.into_body().collect().await.unwrap();
        let trailers  = collected.trailers().cloned().unwrap_or_default();
        assert_eq!(trailers.get("grpc-status").unwrap(), "5"); // NotFound
    }
}
