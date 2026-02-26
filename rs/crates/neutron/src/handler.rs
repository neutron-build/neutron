//! Core request/response types and the handler trait.
//!
//! Defines [`Request`], [`Response`], [`Body`], [`IntoResponse`], and the
//! [`Handler`] trait that allows async functions with up to twelve extractors
//! to serve as route handlers.
//!
//! ```rust,ignore
//! async fn greet(Path(name): Path<String>) -> impl IntoResponse {
//!     format!("Hello, {name}!")
//! }
//! ```

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::convert::Infallible;
use std::future::Future;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::{BufMut, Bytes, BytesMut};
use http::{HeaderMap, Method, StatusCode, Uri};
use http_body::{Body as HttpBody, Frame, SizeHint};
use http_body_util::Full;
use smallvec::SmallVec;
#[cfg(feature = "json")]
use serde::Serialize;

use crate::extract::FromRequest;

// ---------------------------------------------------------------------------
// Body
// ---------------------------------------------------------------------------

/// Neutron response body — supports both buffered and streaming responses.
///
/// Most handlers return `Body::Full` (a pre-buffered body). Streaming responses
/// (SSE, chunked transfers) use `Body::Stream`.
pub enum Body {
    /// Pre-buffered body (most responses).
    Full(Full<Bytes>),
    /// Streaming body (SSE, WebSocket upgrade, chunked responses).
    Stream(Pin<Box<dyn HttpBody<Data = Bytes, Error = Infallible> + Send>>),
}

impl Body {
    /// Create a full (buffered) body from bytes.
    pub fn full(data: impl Into<Bytes>) -> Self {
        Body::Full(Full::new(data.into()))
    }

    /// Create an empty body.
    pub fn empty() -> Self {
        Body::Full(Full::new(Bytes::new()))
    }

    /// Create a streaming body from any type implementing `http_body::Body`.
    pub fn stream<B>(body: B) -> Self
    where
        B: HttpBody<Data = Bytes, Error = Infallible> + Send + 'static,
    {
        Body::Stream(Box::pin(body))
    }

    /// Returns `true` if this is a streaming body.
    pub fn is_streaming(&self) -> bool {
        matches!(self, Body::Stream(_))
    }
}

impl HttpBody for Body {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match self.get_mut() {
            Body::Full(full) => Pin::new(full).poll_frame(cx),
            Body::Stream(stream) => stream.as_mut().poll_frame(cx),
        }
    }

    fn is_end_stream(&self) -> bool {
        match self {
            Body::Full(full) => full.is_end_stream(),
            Body::Stream(_) => false,
        }
    }

    fn size_hint(&self) -> SizeHint {
        match self {
            Body::Full(full) => full.size_hint(),
            Body::Stream(_) => SizeHint::default(),
        }
    }
}

impl From<Full<Bytes>> for Body {
    fn from(full: Full<Bytes>) -> Self {
        Body::Full(full)
    }
}

impl From<Bytes> for Body {
    fn from(bytes: Bytes) -> Self {
        Body::full(bytes)
    }
}

impl From<String> for Body {
    fn from(s: String) -> Self {
        Body::full(s)
    }
}

impl From<&'static str> for Body {
    fn from(s: &'static str) -> Self {
        Body::full(s)
    }
}

impl From<Vec<u8>> for Body {
    fn from(v: Vec<u8>) -> Self {
        Body::full(v)
    }
}

/// HTTP response type used throughout Neutron.
pub type Response = http::Response<Body>;

// ---------------------------------------------------------------------------
// Type-erased state storage
// ---------------------------------------------------------------------------

/// Helper trait so we can downcast `Arc<dyn AnyState>` back to concrete types.
pub trait AnyState: Send + Sync + 'static {
    fn as_any(&self) -> &dyn Any;
}

impl<T: Send + Sync + 'static> AnyState for T {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Map from `TypeId` → type-erased value. Shared across all requests via `Arc`.
pub type StateMap = HashMap<TypeId, Arc<dyn AnyState>>;

/// Builder for constructing a [`StateMap`] to inject into a synthetic [`Request`].
///
/// Used by addon crates (`neutron-jobs`, etc.) to build requests outside the HTTP server.
///
/// ```ignore
/// let state = StateMapBuilder::new()
///     .insert(AppDb::connect().await)
///     .insert(AppConfig::default())
///     .build();
/// ```
pub struct StateMapBuilder(HashMap<TypeId, Arc<dyn AnyState>>);

impl StateMapBuilder {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    /// Insert a value of type `T` into the state map.
    pub fn insert<T: Send + Sync + 'static>(mut self, value: T) -> Self {
        self.0.insert(TypeId::of::<T>(), Arc::new(value) as Arc<dyn AnyState>);
        self
    }

    /// Finalize and return the state map wrapped in an `Arc`.
    pub fn build(self) -> Arc<StateMap> {
        Arc::new(self.0)
    }
}

impl Default for StateMapBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Request
// ---------------------------------------------------------------------------

/// Neutron request — holds all data extractors need.
///
/// Per-request extensions (set by middleware, read by handlers) are stored in
/// a `SmallVec` with inline capacity for 4 entries. The common case — 0 to 3
/// extensions such as request ID, trace ID, and auth claims — requires zero
/// heap allocation. A fifth or later extension spills to the heap exactly once.
pub struct Request {
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
    params: SmallVec<[(String, String); 4]>,
    state: Arc<StateMap>,
    on_upgrade: std::sync::Mutex<Option<hyper::upgrade::OnUpgrade>>,
    extensions: SmallVec<[(TypeId, Box<dyn Any + Send + Sync>); 4]>,
    remote_addr: Option<SocketAddr>,
}

impl Request {
    pub fn new(method: Method, uri: Uri, headers: HeaderMap, body: Bytes) -> Self {
        Self {
            method,
            uri,
            headers,
            body,
            params: SmallVec::new(),
            state: Arc::new(HashMap::new()),
            on_upgrade: std::sync::Mutex::new(None),
            extensions: SmallVec::new(),
            remote_addr: None,
        }
    }

    /// Create a new request with pre-built state — avoids allocating a temporary
    /// empty `Arc<StateMap>` that would immediately be overwritten.  Used by all
    /// production server paths; `new()` is kept for `TestClient` and user code.
    pub(crate) fn with_state(
        method: Method,
        uri: Uri,
        headers: HeaderMap,
        body: Bytes,
        state: Arc<StateMap>,
    ) -> Self {
        Self {
            method,
            uri,
            headers,
            body,
            params: SmallVec::new(),
            state,
            on_upgrade: std::sync::Mutex::new(None),
            extensions: SmallVec::new(),
            remote_addr: None,
        }
    }

    pub fn method(&self) -> &Method {
        &self.method
    }

    pub fn uri(&self) -> &Uri {
        &self.uri
    }

    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    pub fn body(&self) -> &Bytes {
        &self.body
    }

    pub fn params(&self) -> &[(String, String)] {
        &self.params
    }

    pub(crate) fn set_params(&mut self, params: SmallVec<[(String, String); 4]>) {
        self.params = params;
    }

    pub fn set_state(&mut self, state: Arc<StateMap>) {
        self.state = state;
    }

    pub(crate) fn set_on_upgrade(&mut self, on_upgrade: hyper::upgrade::OnUpgrade) {
        *self.on_upgrade.get_mut().unwrap() = Some(on_upgrade);
    }

    /// Take the HTTP upgrade future (used by WebSocket extractor).
    #[cfg(feature = "ws")]
    pub(crate) fn take_on_upgrade(&self) -> Option<hyper::upgrade::OnUpgrade> {
        self.on_upgrade.lock().unwrap().take()
    }

    /// Set the remote (client) address.
    pub(crate) fn set_remote_addr(&mut self, addr: SocketAddr) {
        self.remote_addr = Some(addr);
    }

    /// Get the remote (client) socket address, if available.
    pub fn remote_addr(&self) -> Option<SocketAddr> {
        self.remote_addr
    }

    /// Retrieve a shared state value by type.
    pub fn get_state<T: Send + Sync + 'static>(&self) -> Option<&T> {
        self.state
            .get(&TypeId::of::<T>())
            .and_then(|arc| (**arc).as_any().downcast_ref::<T>())
    }

    /// Set a per-request extension value (used by middleware to pass data to handlers).
    ///
    /// If a value of the same type already exists it is replaced in-place,
    /// preserving slot order. The first four distinct extension types require
    /// no heap allocation (inline `SmallVec` storage).
    pub fn set_extension<T: Send + Sync + 'static>(&mut self, value: T) {
        let id = TypeId::of::<T>();
        if let Some(slot) = self.extensions.iter_mut().find(|(tid, _)| *tid == id) {
            slot.1 = Box::new(value);
        } else {
            self.extensions.push((id, Box::new(value)));
        }
    }

    /// Get a per-request extension value by type.
    pub fn get_extension<T: Send + Sync + 'static>(&self) -> Option<&T> {
        let id = TypeId::of::<T>();
        self.extensions
            .iter()
            .find(|(tid, _)| *tid == id)
            .and_then(|(_, v)| v.downcast_ref::<T>())
    }
}

/// Trait for types that can be converted into an HTTP response.
pub trait IntoResponse {
    fn into_response(self) -> Response;
}

impl IntoResponse for Response {
    fn into_response(self) -> Response {
        self
    }
}

impl IntoResponse for &'static str {
    fn into_response(self) -> Response {
        http::Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/plain; charset=utf-8")
            .body(Body::full(self))
            .unwrap()
    }
}

impl IntoResponse for String {
    fn into_response(self) -> Response {
        http::Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/plain; charset=utf-8")
            .body(Body::full(self))
            .unwrap()
    }
}

/// JSON wrapper — usable as both a response type and an extractor.
///
/// Requires the `json` feature (enabled by default via `full` or `web`).
#[cfg(feature = "json")]
pub struct Json<T>(pub T);

#[cfg(feature = "json")]
impl<T: Serialize> IntoResponse for Json<T> {
    fn into_response(self) -> Response {
        // Use BytesMut + to_writer + freeze() instead of to_vec() + Bytes::from().
        //
        // to_vec(): allocates Vec<u8>  +  Bytes::from(vec): allocates Arc wrapper = 2 allocs.
        // BytesMut::with_capacity():  1 alloc (Arc already inside BytesMut).
        // freeze():                   0 alloc (atomically drops mut permission, same Arc).
        //
        // Net saving: 1 allocation per JSON response.
        let mut buf = BytesMut::with_capacity(256);
        match serde_json::to_writer((&mut buf).writer(), &self.0) {
            Ok(()) => http::Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(Body::full(buf.freeze()))
                .unwrap(),
            Err(e) => http::Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .header("content-type", "text/plain; charset=utf-8")
                .body(Body::full(format!("JSON serialization error: {e}")))
                .unwrap(),
        }
    }
}

impl IntoResponse for StatusCode {
    fn into_response(self) -> Response {
        http::Response::builder()
            .status(self)
            .body(Body::empty())
            .unwrap()
    }
}

impl IntoResponse for Bytes {
    fn into_response(self) -> Response {
        http::Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/octet-stream")
            .body(Body::full(self))
            .unwrap()
    }
}

impl IntoResponse for Vec<u8> {
    fn into_response(self) -> Response {
        Bytes::from(self).into_response()
    }
}

impl IntoResponse for () {
    fn into_response(self) -> Response {
        StatusCode::OK.into_response()
    }
}

impl<T: IntoResponse> IntoResponse for (StatusCode, T) {
    fn into_response(self) -> Response {
        let mut resp = self.1.into_response();
        *resp.status_mut() = self.0;
        resp
    }
}

impl<T: IntoResponse> IntoResponse for (HeaderMap, T) {
    fn into_response(self) -> Response {
        let (headers, body) = self;
        let mut resp = body.into_response();
        resp.headers_mut().extend(headers);
        resp
    }
}

impl<T: IntoResponse> IntoResponse for (StatusCode, HeaderMap, T) {
    fn into_response(self) -> Response {
        let (status, headers, body) = self;
        let mut resp = body.into_response();
        *resp.status_mut() = status;
        resp.headers_mut().extend(headers);
        resp
    }
}

impl<T: IntoResponse, E: IntoResponse> IntoResponse for Result<T, E> {
    fn into_response(self) -> Response {
        match self {
            Ok(v) => v.into_response(),
            Err(e) => e.into_response(),
        }
    }
}

// ---------------------------------------------------------------------------
// Redirect
// ---------------------------------------------------------------------------

/// HTTP redirect response.
///
/// ```rust,ignore
/// async fn old_page() -> Redirect {
///     Redirect::to("/new-page")
/// }
/// ```
pub struct Redirect {
    status: StatusCode,
    location: String,
}

impl Redirect {
    /// 303 See Other — the standard redirect for POST→GET flows.
    pub fn to(uri: &str) -> Self {
        Self {
            status: StatusCode::SEE_OTHER,
            location: uri.to_string(),
        }
    }

    /// 301 Moved Permanently — the URL has permanently changed.
    pub fn permanent(uri: &str) -> Self {
        Self {
            status: StatusCode::MOVED_PERMANENTLY,
            location: uri.to_string(),
        }
    }

    /// 307 Temporary Redirect — preserves the HTTP method.
    pub fn temporary(uri: &str) -> Self {
        Self {
            status: StatusCode::TEMPORARY_REDIRECT,
            location: uri.to_string(),
        }
    }
}

impl IntoResponse for Redirect {
    fn into_response(self) -> Response {
        http::Response::builder()
            .status(self.status)
            .header("location", self.location)
            .body(Body::empty())
            .unwrap()
    }
}

// ---------------------------------------------------------------------------
// Handler system
// ---------------------------------------------------------------------------

/// Async handler trait — parameterized by extractor tuple `T`.
pub trait Handler<T>: Send + Sync + 'static {
    fn call(&self, req: Request) -> Pin<Box<dyn Future<Output = Response> + Send>>;
}

/// Type-erased handler stored in the router.
pub(crate) trait ErasedHandler: Send + Sync {
    fn call(&self, req: Request) -> Pin<Box<dyn Future<Output = Response> + Send>>;
}

/// Boxed type-erased handler.
pub(crate) type BoxedHandler = Box<dyn ErasedHandler>;

struct HandlerWrapper<H, T> {
    handler: H,
    _marker: PhantomData<fn() -> T>,
}

unsafe impl<H: Send, T> Send for HandlerWrapper<H, T> {}
unsafe impl<H: Sync, T> Sync for HandlerWrapper<H, T> {}

impl<H, T> ErasedHandler for HandlerWrapper<H, T>
where
    H: Handler<T>,
    T: 'static,
{
    fn call(&self, req: Request) -> Pin<Box<dyn Future<Output = Response> + Send>> {
        self.handler.call(req)
    }
}

/// Box a typed handler into a type-erased [`BoxedHandler`].
pub(crate) fn into_boxed<H, T>(handler: H) -> BoxedHandler
where
    H: Handler<T>,
    T: 'static,
{
    Box::new(HandlerWrapper {
        handler,
        _marker: PhantomData,
    })
}

// ---------------------------------------------------------------------------
// Handler implementations for async fns with 0–12 extractors
// ---------------------------------------------------------------------------

// Zero extractors: async fn() -> impl IntoResponse
impl<F, Fut, Res> Handler<()> for F
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Res> + Send + 'static,
    Res: IntoResponse,
{
    fn call(&self, _req: Request) -> Pin<Box<dyn Future<Output = Response> + Send>> {
        let fut = (self)();
        Box::pin(async move { fut.await.into_response() })
    }
}

// Macro to generate Handler impls for 1–12 extractors.
macro_rules! impl_handler {
    ($($T:ident),+) => {
        #[allow(non_snake_case)]
        impl<F, Fut, Res, $($T,)+> Handler<($($T,)+)> for F
        where
            F: Fn($($T,)+) -> Fut + Send + Sync + 'static,
            Fut: Future<Output = Res> + Send + 'static,
            Res: IntoResponse,
            $($T: FromRequest + 'static,)+
        {
            fn call(&self, req: Request) -> Pin<Box<dyn Future<Output = Response> + Send>> {
                $(
                    let $T = match $T::from_request(&req) {
                        Ok(v) => v,
                        Err(e) => return Box::pin(async move { e }),
                    };
                )+
                let fut = (self)($($T,)+);
                Box::pin(async move { fut.await.into_response() })
            }
        }
    };
}

impl_handler!(T1);
impl_handler!(T1, T2);
impl_handler!(T1, T2, T3);
impl_handler!(T1, T2, T3, T4);
impl_handler!(T1, T2, T3, T4, T5);
impl_handler!(T1, T2, T3, T4, T5, T6);
impl_handler!(T1, T2, T3, T4, T5, T6, T7);
impl_handler!(T1, T2, T3, T4, T5, T6, T7, T8);
impl_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9);
impl_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10);
impl_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11);
impl_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12);

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

    async fn body_bytes(resp: Response) -> Vec<u8> {
        resp.into_body().collect().await.unwrap().to_bytes().to_vec()
    }

    #[tokio::test]
    async fn static_str_into_response() {
        let resp = "hello".into_response();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get("content-type").unwrap(),
            "text/plain; charset=utf-8"
        );
        assert_eq!(body_bytes(resp).await, b"hello");
    }

    #[tokio::test]
    async fn string_into_response() {
        let resp = String::from("world").into_response();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(body_bytes(resp).await, b"world");
    }

    #[tokio::test]
    async fn status_code_into_response() {
        let resp = StatusCode::NO_CONTENT.into_response();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
        assert!(body_bytes(resp).await.is_empty());
    }

    #[tokio::test]
    async fn unit_into_response() {
        let resp = ().into_response();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(body_bytes(resp).await.is_empty());
    }

    #[tokio::test]
    async fn bytes_into_response() {
        let resp = Bytes::from_static(b"\x00\x01\x02").into_response();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get("content-type").unwrap(),
            "application/octet-stream"
        );
        assert_eq!(body_bytes(resp).await, b"\x00\x01\x02");
    }

    #[tokio::test]
    async fn vec_u8_into_response() {
        let resp = vec![10u8, 20, 30].into_response();
        assert_eq!(body_bytes(resp).await, &[10, 20, 30]);
    }

    #[tokio::test]
    async fn status_body_tuple() {
        let resp = (StatusCode::CREATED, "created").into_response();
        assert_eq!(resp.status(), StatusCode::CREATED);
        assert_eq!(body_bytes(resp).await, b"created");
    }

    #[tokio::test]
    async fn headermap_body_tuple() {
        let mut headers = HeaderMap::new();
        headers.insert("x-custom", "value".parse().unwrap());
        let resp = (headers, "body").into_response();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.headers().get("x-custom").unwrap(), "value");
        assert_eq!(body_bytes(resp).await, b"body");
    }

    #[tokio::test]
    async fn status_headermap_body_tuple() {
        let mut headers = HeaderMap::new();
        headers.insert("x-req-id", "abc123".parse().unwrap());
        let resp = (StatusCode::ACCEPTED, headers, "accepted").into_response();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        assert_eq!(resp.headers().get("x-req-id").unwrap(), "abc123");
        assert_eq!(body_bytes(resp).await, b"accepted");
    }

    #[tokio::test]
    async fn result_ok_into_response() {
        let resp: Result<&str, &str> = Ok("success");
        let resp = resp.into_response();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(body_bytes(resp).await, b"success");
    }

    #[tokio::test]
    async fn result_err_into_response() {
        let resp: Result<&str, (StatusCode, &str)> =
            Err((StatusCode::BAD_REQUEST, "bad request"));
        let resp = resp.into_response();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert_eq!(body_bytes(resp).await, b"bad request");
    }

    #[cfg(feature = "json")]
    #[tokio::test]
    async fn json_into_response() {
        #[derive(serde::Serialize)]
        struct Msg {
            msg: String,
        }
        let resp = Json(Msg {
            msg: "hi".into(),
        })
        .into_response();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get("content-type").unwrap(),
            "application/json"
        );
        assert_eq!(body_bytes(resp).await, br#"{"msg":"hi"}"#);
    }

    #[tokio::test]
    async fn redirect_to() {
        let resp = Redirect::to("/new").into_response();
        assert_eq!(resp.status(), StatusCode::SEE_OTHER);
        assert_eq!(resp.headers().get("location").unwrap(), "/new");
    }

    #[tokio::test]
    async fn redirect_permanent() {
        let resp = Redirect::permanent("/moved").into_response();
        assert_eq!(resp.status(), StatusCode::MOVED_PERMANENTLY);
        assert_eq!(resp.headers().get("location").unwrap(), "/moved");
    }

    #[tokio::test]
    async fn redirect_temporary() {
        let resp = Redirect::temporary("/temp").into_response();
        assert_eq!(resp.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(resp.headers().get("location").unwrap(), "/temp");
    }

    #[tokio::test]
    async fn streaming_body() {
        use http_body_util::StreamBody;

        let stream = tokio_stream::iter(vec![
            Ok::<_, Infallible>(Frame::data(Bytes::from("hello "))),
            Ok(Frame::data(Bytes::from("world"))),
        ]);

        let resp = http::Response::builder()
            .status(StatusCode::OK)
            .body(Body::stream(StreamBody::new(stream)))
            .unwrap();

        assert!(resp.body().is_streaming());
        let collected = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(collected, "hello world");
    }
}
