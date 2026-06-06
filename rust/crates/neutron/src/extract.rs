//! Request data extractors.
//!
//! Extractors pull typed data out of incoming requests — path parameters,
//! query strings, JSON bodies, headers, shared state, and more.
//!
//! Two extractor traits exist so the type system can enforce which extractors
//! consume the body and which don't:
//!
//! * [`FromRequestParts`] — body-free, sync. Implement this for extractors
//!   that only inspect headers, path params, state, or extensions.
//!   A blanket impl automatically provides [`FromRequest`] for free.
//!
//! * [`FromRequest`] — body-consuming. Implement this directly only when your
//!   extractor needs to read the request body (`Bytes`, `String`, `Json<T>`,
//!   `Form<T>`).
//!
//! ```rust,ignore
//! // Body-free custom extractor — implement FromRequestParts
//! struct UserId(u64);
//!
//! impl FromRequestParts for UserId {
//!     fn from_parts(req: &Request) -> Result<Self, Response> {
//!         // read from extension set by auth middleware
//!         req.get_extension::<UserId>()
//!            .cloned()
//!            .ok_or_else(|| (StatusCode::UNAUTHORIZED, "Not authenticated").into_response())
//!     }
//! }
//!
//! // Works in any handler automatically — blanket impl gives you FromRequest
//! async fn profile(UserId(id): UserId, Json(body): Json<Update>) -> impl IntoResponse { ... }
//! ```

use std::fmt;
use std::net::SocketAddr;
use std::str::FromStr;

use bytes::Bytes;
use http::{HeaderMap, Method, StatusCode, Uri};

#[cfg(any(feature = "json", feature = "form"))]
use serde::de::DeserializeOwned;

use crate::handler::{IntoResponse, Request, Response};
#[cfg(feature = "json")]
use crate::handler::Json;

// ---------------------------------------------------------------------------
// Core extractor traits
// ---------------------------------------------------------------------------

/// Extractor trait for types that do **not** consume the request body.
///
/// Implement this for extractors that only need access to request parts:
/// path params, query string, headers, state, and middleware-set extensions.
///
/// A blanket impl automatically satisfies [`FromRequest`] for all
/// `FromRequestParts` types — no extra code required.
pub trait FromRequestParts: Sized + Send {
    fn from_parts(req: &Request) -> Result<Self, Response>;
}

/// Extractor trait for types that may consume the request body.
///
/// All [`FromRequestParts`] types automatically implement this trait via the
/// blanket impl below. Only implement `FromRequest` directly when you need
/// access to the buffered body bytes (`Bytes`, `String`, `Json<T>`, etc.).
pub trait FromRequest: Sized + Send {
    fn from_request(req: &Request) -> Result<Self, Response>;
}

/// Blanket impl: every body-free extractor is also a full extractor.
///
/// This means the handler macro only ever needs `T: FromRequest` — handlers
/// can freely mix `Path`, `State`, `Extension`, `Json`, `Form`, etc. without
/// any special casing.
impl<T: FromRequestParts> FromRequest for T {
    #[inline]
    fn from_request(req: &Request) -> Result<Self, Response> {
        T::from_parts(req)
    }
}

// ---------------------------------------------------------------------------
// Path extractor
// ---------------------------------------------------------------------------

/// Extract path parameters from the URL.
///
/// For a single param: `Path(id): Path<u64>`
/// For multiple params: `Path((org, repo)): Path<(String, String)>`
pub struct Path<T>(pub T);

/// Trait for types that can be deserialized from path parameters.
pub trait PathParam: Sized {
    fn from_params(params: &[(String, String)]) -> Result<Self, String>;
}

// Implement PathParam for common scalar types via macro
macro_rules! impl_path_param_scalar {
    ($($ty:ty),*) => {
        $(
            impl PathParam for $ty {
                fn from_params(params: &[(String, String)]) -> Result<Self, String> {
                    let (_, value) = params
                        .first()
                        .ok_or_else(|| "missing path parameter".to_string())?;
                    value
                        .parse::<$ty>()
                        .map_err(|e| format!("invalid path parameter: {e}"))
                }
            }
        )*
    };
}

impl_path_param_scalar!(u8, u16, u32, u64, u128, i8, i16, i32, i64, i128, f32, f64, bool, String);

// Two-element tuple
impl<A, B> PathParam for (A, B)
where
    A: FromStr,
    A::Err: fmt::Display,
    B: FromStr,
    B::Err: fmt::Display,
{
    fn from_params(params: &[(String, String)]) -> Result<Self, String> {
        if params.len() < 2 {
            return Err(format!(
                "expected 2 path parameters, got {}",
                params.len()
            ));
        }
        let a = params[0]
            .1
            .parse::<A>()
            .map_err(|e| format!("invalid path param 1: {e}"))?;
        let b = params[1]
            .1
            .parse::<B>()
            .map_err(|e| format!("invalid path param 2: {e}"))?;
        Ok((a, b))
    }
}

// Three-element tuple
impl<A, B, C> PathParam for (A, B, C)
where
    A: FromStr,
    A::Err: fmt::Display,
    B: FromStr,
    B::Err: fmt::Display,
    C: FromStr,
    C::Err: fmt::Display,
{
    fn from_params(params: &[(String, String)]) -> Result<Self, String> {
        if params.len() < 3 {
            return Err(format!(
                "expected 3 path parameters, got {}",
                params.len()
            ));
        }
        let a = params[0]
            .1
            .parse::<A>()
            .map_err(|e| format!("invalid path param 1: {e}"))?;
        let b = params[1]
            .1
            .parse::<B>()
            .map_err(|e| format!("invalid path param 2: {e}"))?;
        let c = params[2]
            .1
            .parse::<C>()
            .map_err(|e| format!("invalid path param 3: {e}"))?;
        Ok((a, b, c))
    }
}

impl<T: PathParam + Send + 'static> FromRequestParts for Path<T> {
    fn from_parts(req: &Request) -> Result<Self, Response> {
        T::from_params(req.params())
            .map(Path)
            .map_err(|msg| (StatusCode::BAD_REQUEST, msg).into_response())
    }
}

// ---------------------------------------------------------------------------
// Query extractor — requires feature = "form" (uses serde + serde_urlencoded)
// ---------------------------------------------------------------------------

/// Extract query parameters from the URL query string.
///
/// Requires the `form` feature (enabled by default via `full` or `web`).
#[cfg(feature = "form")]
pub struct Query<T>(pub T);

#[cfg(feature = "form")]
impl<T: DeserializeOwned + Send + 'static> FromRequestParts for Query<T> {
    fn from_parts(req: &Request) -> Result<Self, Response> {
        let query = req.uri().query().unwrap_or("");
        serde_urlencoded::from_str(query)
            .map(Query)
            .map_err(|e| {
                (StatusCode::BAD_REQUEST, format!("Invalid query: {e}")).into_response()
            })
    }
}

// ---------------------------------------------------------------------------
// State extractor
// ---------------------------------------------------------------------------

/// Extract shared application state.
///
/// Register state on the router with [`Router::state()`], then extract it in
/// any handler:
///
/// ```ignore
/// async fn handler(State(cfg): State<AppConfig>) -> String {
///     format!("app = {}", cfg.name)
/// }
/// ```
pub struct State<T>(pub T);

impl<T: Clone + Send + Sync + 'static> FromRequestParts for State<T> {
    fn from_parts(req: &Request) -> Result<Self, Response> {
        req.get_state::<T>()
            .cloned()
            .map(State)
            .ok_or_else(|| {
                tracing::error!(
                    "State<{}> not found — did you call Router::state()?",
                    std::any::type_name::<T>()
                );
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal Server Error",
                )
                    .into_response()
            })
    }
}

// ---------------------------------------------------------------------------
// Simple extractors: Method, Uri, HeaderMap
// ---------------------------------------------------------------------------

impl FromRequestParts for Method {
    fn from_parts(req: &Request) -> Result<Self, Response> {
        Ok(req.method().clone())
    }
}

impl FromRequestParts for Uri {
    fn from_parts(req: &Request) -> Result<Self, Response> {
        Ok(req.uri().clone())
    }
}

impl FromRequestParts for HeaderMap {
    fn from_parts(req: &Request) -> Result<Self, Response> {
        Ok(req.headers().clone())
    }
}

// ---------------------------------------------------------------------------
// ConnectInfo extractor (client address)
// ---------------------------------------------------------------------------

/// Extract the client's remote socket address.
///
/// ```rust,ignore
/// async fn handler(ConnectInfo(addr): ConnectInfo) -> String {
///     format!("Your IP: {}", addr.ip())
/// }
/// ```
pub struct ConnectInfo(pub SocketAddr);

impl FromRequestParts for ConnectInfo {
    fn from_parts(req: &Request) -> Result<Self, Response> {
        req.remote_addr()
            .map(ConnectInfo)
            .ok_or_else(|| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Remote address not available",
                )
                    .into_response()
            })
    }
}

// ---------------------------------------------------------------------------
// Extension extractor (per-request data from middleware)
// ---------------------------------------------------------------------------

/// Extract a per-request extension value set by middleware.
///
/// ```rust,ignore
/// // In middleware:
/// req.set_extension(UserId(42));
///
/// // In handler:
/// async fn handler(Extension(user_id): Extension<UserId>) -> String {
///     format!("User: {}", user_id.0)
/// }
/// ```
pub struct Extension<T>(pub T);

impl<T: Clone + Send + Sync + 'static> FromRequestParts for Extension<T> {
    fn from_parts(req: &Request) -> Result<Self, Response> {
        req.get_extension::<T>()
            .cloned()
            .map(Extension)
            .ok_or_else(|| {
                tracing::error!(
                    "Extension<{}> not found — did middleware set it?",
                    std::any::type_name::<T>()
                );
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal Server Error",
                )
                    .into_response()
            })
    }
}

// ---------------------------------------------------------------------------
// Body extractors: Bytes, String
// ---------------------------------------------------------------------------

/// Raw request body as bytes.
impl FromRequest for Bytes {
    fn from_request(req: &Request) -> Result<Self, Response> {
        Ok(req.body().clone())
    }
}

/// Request body decoded as UTF-8 text.
impl FromRequest for String {
    fn from_request(req: &Request) -> Result<Self, Response> {
        String::from_utf8(req.body().to_vec()).map_err(|_| {
            (StatusCode::BAD_REQUEST, "Request body is not valid UTF-8").into_response()
        })
    }
}

// ---------------------------------------------------------------------------
// Json extractor (body deserialization) — requires feature = "json"
// ---------------------------------------------------------------------------

#[cfg(feature = "json")]
impl<T: DeserializeOwned + Send + 'static> FromRequest for Json<T> {
    fn from_request(req: &Request) -> Result<Self, Response> {
        let content_type = req
            .headers()
            .get(http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        if !content_type.starts_with("application/json") {
            return Err(
                (StatusCode::UNSUPPORTED_MEDIA_TYPE, "Expected application/json").into_response(),
            );
        }

        json_from_slice(req.body())
            .map(Json)
            .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid JSON: {e}")).into_response())
    }
}

/// Parse JSON bytes — uses simd-json when the feature is enabled, otherwise serde_json.
#[cfg(feature = "json")]
fn json_from_slice<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    #[cfg(feature = "simd-json")]
    {
        // simd-json::from_slice requires a mutable slice (it modifies bytes in-place).
        let mut owned = bytes.to_vec();
        simd_json::from_slice(&mut owned).map_err(|e| e.to_string())
    }
    #[cfg(not(feature = "simd-json"))]
    {
        serde_json::from_slice(bytes).map_err(|e| e.to_string())
    }
}

// ---------------------------------------------------------------------------
// Form extractor — requires feature = "form"
// ---------------------------------------------------------------------------

/// Extract form data from the request body (`application/x-www-form-urlencoded`).
///
/// Requires the `form` feature (enabled by default via `full` or `web`).
///
/// ```rust,ignore
/// async fn login(Form(creds): Form<LoginForm>) -> impl IntoResponse {
///     // creds.username, creds.password
/// }
/// ```
#[cfg(feature = "form")]
pub struct Form<T>(pub T);

#[cfg(feature = "form")]
impl<T: DeserializeOwned + Send + 'static> FromRequest for Form<T> {
    fn from_request(req: &Request) -> Result<Self, Response> {
        let content_type = req
            .headers()
            .get(http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        if !content_type.starts_with("application/x-www-form-urlencoded") {
            return Err((
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "Expected application/x-www-form-urlencoded",
            )
                .into_response());
        }

        serde_urlencoded::from_bytes(req.body())
            .map(Form)
            .map_err(|e| {
                (StatusCode::BAD_REQUEST, format!("Invalid form data: {e}")).into_response()
            })
    }
}

// ---------------------------------------------------------------------------
// Optional extractor — wraps any FromRequestParts to make it non-failing
// ---------------------------------------------------------------------------

/// Wraps any [`FromRequestParts`] extractor to make extraction optional.
///
/// If the inner extractor succeeds, `Optional(Some(value))` is returned.
/// If it fails (missing header, bad parse, etc.), `Optional(None)` is returned
/// instead of propagating the error — the handler still runs.
///
/// ```rust,ignore
/// async fn handler(
///     Optional(auth): Optional<TypedHeader<Authorization>>,
/// ) -> impl IntoResponse {
///     match auth {
///         Some(TypedHeader(Authorization(token))) => format!("Authenticated: {token}"),
///         None => "Anonymous".to_string(),
///     }
/// }
/// ```
pub struct Optional<T>(pub Option<T>);

impl<T: FromRequestParts> FromRequestParts for Optional<T> {
    fn from_parts(req: &Request) -> Result<Self, Response> {
        match T::from_parts(req) {
            Ok(value) => Ok(Optional(Some(value))),
            Err(_) => Ok(Optional(None)),
        }
    }
}

// ---------------------------------------------------------------------------
// TypedHeader extractor — type-safe single-header extraction
// ---------------------------------------------------------------------------

/// Trait for header types that know their HTTP header name and how to decode
/// from a raw [`http::HeaderValue`].
///
/// Implement this for custom header types:
///
/// ```rust,ignore
/// pub struct XRequestId(pub String);
///
/// impl TypedHeaderValue for XRequestId {
///     const HEADER_NAME: &'static str = "x-request-id";
///
///     fn decode(value: &http::HeaderValue) -> Result<Self, String> {
///         value.to_str()
///             .map(|s| XRequestId(s.to_owned()))
///             .map_err(|e| format!("invalid x-request-id: {e}"))
///     }
/// }
/// ```
pub trait TypedHeaderValue: Sized + Send + Sync {
    /// The HTTP header name this type maps to (lowercase, e.g. `"content-type"`).
    const HEADER_NAME: &'static str;

    /// Decode a concrete value from the raw header bytes.
    fn decode(value: &http::HeaderValue) -> Result<Self, String>;
}

/// Extract a single, strongly-typed HTTP header from the request.
///
/// Returns a 400 Bad Request if the header is missing or cannot be decoded.
/// Wrap in [`Optional`] to make it non-failing:
///
/// ```rust,ignore
/// async fn handler(
///     TypedHeader(ct): TypedHeader<ContentType>,
///     Optional(auth): Optional<TypedHeader<BearerToken>>,
/// ) -> String {
///     format!("Content-Type: {}, auth: {:?}", ct.0, auth.map(|h| h.0))
/// }
/// ```
pub struct TypedHeader<T: TypedHeaderValue>(pub T);

impl<T: TypedHeaderValue + 'static> FromRequestParts for TypedHeader<T> {
    fn from_parts(req: &Request) -> Result<Self, Response> {
        let value = req
            .headers()
            .get(T::HEADER_NAME)
            .ok_or_else(|| {
                (
                    StatusCode::BAD_REQUEST,
                    format!("Missing required header: {}", T::HEADER_NAME),
                )
                    .into_response()
            })?;

        T::decode(value)
            .map(TypedHeader)
            .map_err(|msg| {
                (
                    StatusCode::BAD_REQUEST,
                    format!("Invalid header {}: {}", T::HEADER_NAME, msg),
                )
                    .into_response()
            })
    }
}

// ---------------------------------------------------------------------------
// Built-in TypedHeaderValue implementations
// ---------------------------------------------------------------------------

/// The `Content-Type` header value as a string.
pub struct ContentType(pub String);

impl TypedHeaderValue for ContentType {
    const HEADER_NAME: &'static str = "content-type";

    fn decode(value: &http::HeaderValue) -> Result<Self, String> {
        value
            .to_str()
            .map(|s| ContentType(s.to_owned()))
            .map_err(|e| format!("non-ASCII header value: {e}"))
    }
}

/// The `Authorization` header value as a raw string (includes scheme prefix).
pub struct Authorization(pub String);

impl TypedHeaderValue for Authorization {
    const HEADER_NAME: &'static str = "authorization";

    fn decode(value: &http::HeaderValue) -> Result<Self, String> {
        value
            .to_str()
            .map(|s| Authorization(s.to_owned()))
            .map_err(|e| format!("non-ASCII header value: {e}"))
    }
}

/// The `Accept` header value as a raw string.
pub struct Accept(pub String);

impl TypedHeaderValue for Accept {
    const HEADER_NAME: &'static str = "accept";

    fn decode(value: &http::HeaderValue) -> Result<Self, String> {
        value
            .to_str()
            .map(|s| Accept(s.to_owned()))
            .map_err(|e| format!("non-ASCII header value: {e}"))
    }
}

/// The `User-Agent` header value.
pub struct UserAgent(pub String);

impl TypedHeaderValue for UserAgent {
    const HEADER_NAME: &'static str = "user-agent";

    fn decode(value: &http::HeaderValue) -> Result<Self, String> {
        value
            .to_str()
            .map(|s| UserAgent(s.to_owned()))
            .map_err(|e| format!("non-ASCII header value: {e}"))
    }
}

/// The `Host` header value.
pub struct Host(pub String);

impl TypedHeaderValue for Host {
    const HEADER_NAME: &'static str = "host";

    fn decode(value: &http::HeaderValue) -> Result<Self, String> {
        value
            .to_str()
            .map(|s| Host(s.to_owned()))
            .map_err(|e| format!("non-ASCII header value: {e}"))
    }
}

/// The `Origin` header value.
pub struct Origin(pub String);

impl TypedHeaderValue for Origin {
    const HEADER_NAME: &'static str = "origin";

    fn decode(value: &http::HeaderValue) -> Result<Self, String> {
        value
            .to_str()
            .map(|s| Origin(s.to_owned()))
            .map_err(|e| format!("non-ASCII header value: {e}"))
    }
}

/// A bearer token extracted from the `Authorization` header.
///
/// Strips the `"Bearer "` prefix and returns only the token string.
/// Returns an error if the header does not start with `"Bearer "`.
pub struct BearerToken(pub String);

impl TypedHeaderValue for BearerToken {
    const HEADER_NAME: &'static str = "authorization";

    fn decode(value: &http::HeaderValue) -> Result<Self, String> {
        let s = value
            .to_str()
            .map_err(|e| format!("non-ASCII header value: {e}"))?;

        s.strip_prefix("Bearer ")
            .map(|token| BearerToken(token.to_owned()))
            .ok_or_else(|| {
                "Authorization header does not start with \"Bearer \"".to_string()
            })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler::{Request, StateMapBuilder};
    use bytes::Bytes;
    use http::{HeaderMap, HeaderValue, Method, StatusCode, Uri};
    use smallvec::SmallVec;

    /// Helper: build a minimal GET request.
    fn get_request(uri: &str) -> Request {
        Request::new(
            Method::GET,
            uri.parse::<Uri>().unwrap(),
            HeaderMap::new(),
            Bytes::new(),
        )
    }

    /// Helper: build a request with params already set.
    fn request_with_params(params: Vec<(String, String)>) -> Request {
        let mut req = get_request("/test");
        let sv: SmallVec<[(String, String); 4]> = params.into();
        req.set_params(sv);
        req
    }

    // --- PathParam scalar parsing ---

    #[test]
    fn path_param_u64_single() {
        let params = vec![("id".into(), "42".into())];
        let result = u64::from_params(&params);
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn path_param_string_single() {
        let params = vec![("name".into(), "hello".into())];
        let result = String::from_params(&params);
        assert_eq!(result.unwrap(), "hello");
    }

    #[test]
    fn path_param_i32_negative() {
        let params = vec![("id".into(), "-5".into())];
        let result = i32::from_params(&params);
        assert_eq!(result.unwrap(), -5);
    }

    #[test]
    fn path_param_bool() {
        let params = vec![("flag".into(), "true".into())];
        let result = bool::from_params(&params);
        assert!(result.unwrap());
    }

    #[test]
    fn path_param_f64() {
        let params = vec![("val".into(), "3.14".into())];
        let result = f64::from_params(&params);
        assert!((result.unwrap() - 3.14).abs() < f64::EPSILON);
    }

    #[test]
    fn path_param_missing() {
        let params: Vec<(String, String)> = vec![];
        let result = u64::from_params(&params);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("missing path parameter"));
    }

    #[test]
    fn path_param_invalid_value() {
        let params = vec![("id".into(), "not_a_number".into())];
        let result = u64::from_params(&params);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("invalid path parameter"));
    }

    // --- PathParam tuple parsing ---

    #[test]
    fn path_param_2_tuple() {
        let params = vec![
            ("org".into(), "neutron".into()),
            ("repo".into(), "core".into()),
        ];
        let result = <(String, String)>::from_params(&params);
        let (a, b) = result.unwrap();
        assert_eq!(a, "neutron");
        assert_eq!(b, "core");
    }

    #[test]
    fn path_param_2_tuple_typed() {
        let params = vec![
            ("org".into(), "neutron".into()),
            ("id".into(), "42".into()),
        ];
        let result = <(String, u64)>::from_params(&params);
        let (name, id) = result.unwrap();
        assert_eq!(name, "neutron");
        assert_eq!(id, 42);
    }

    #[test]
    fn path_param_2_tuple_too_few() {
        let params = vec![("only_one".into(), "value".into())];
        let result = <(String, String)>::from_params(&params);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("expected 2"));
    }

    #[test]
    fn path_param_3_tuple() {
        let params = vec![
            ("a".into(), "1".into()),
            ("b".into(), "2".into()),
            ("c".into(), "3".into()),
        ];
        let result = <(u32, u32, u32)>::from_params(&params);
        let (a, b, c) = result.unwrap();
        assert_eq!((a, b, c), (1, 2, 3));
    }

    #[test]
    fn path_param_3_tuple_too_few() {
        let params = vec![
            ("a".into(), "1".into()),
            ("b".into(), "2".into()),
        ];
        let result = <(u32, u32, u32)>::from_params(&params);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("expected 3"));
    }

    // --- Path extractor via FromRequestParts ---

    /// Helper: unwrap a Result<T, Response> without requiring Debug on Response.
    fn ok_or_panic<T>(result: Result<T, Response>) -> T {
        match result {
            Ok(v) => v,
            Err(resp) => panic!("expected Ok, got Err with status {}", resp.status()),
        }
    }

    /// Helper: unwrap_err a Result<T, Response> without requiring Debug on T.
    fn err_or_panic<T>(result: Result<T, Response>) -> Response {
        match result {
            Err(resp) => resp,
            Ok(_) => panic!("expected Err, got Ok"),
        }
    }

    #[test]
    fn path_extractor_success() {
        let req = request_with_params(vec![("id".into(), "99".into())]);
        let Path(id) = ok_or_panic(Path::<u64>::from_parts(&req));
        assert_eq!(id, 99);
    }

    #[test]
    fn path_extractor_invalid_returns_400() {
        let req = request_with_params(vec![("id".into(), "abc".into())]);
        let err = err_or_panic(Path::<u64>::from_parts(&req));
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn path_extractor_missing_returns_400() {
        let req = get_request("/test");
        let err = err_or_panic(Path::<u64>::from_parts(&req));
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }

    // --- Query extractor ---

    #[cfg(feature = "form")]
    #[test]
    fn query_extractor_success() {
        use serde::Deserialize;

        #[derive(Deserialize)]
        struct Params {
            page: u32,
            limit: u32,
        }

        let req = get_request("/items?page=2&limit=10");
        let Query(params) = ok_or_panic(Query::<Params>::from_parts(&req));
        assert_eq!(params.page, 2);
        assert_eq!(params.limit, 10);
    }

    #[cfg(feature = "form")]
    #[test]
    fn query_extractor_empty_query_string() {
        use serde::Deserialize;

        #[derive(Deserialize)]
        struct Params {
            #[serde(default)]
            page: Option<u32>,
        }

        let req = get_request("/items");
        let Query(params) = ok_or_panic(Query::<Params>::from_parts(&req));
        assert!(params.page.is_none());
    }

    #[cfg(feature = "form")]
    #[test]
    fn query_extractor_bad_query_returns_400() {
        use serde::Deserialize;

        #[derive(Deserialize)]
        struct Params {
            page: u32,
        }

        let req = get_request("/items?page=notanumber");
        let err = err_or_panic(Query::<Params>::from_parts(&req));
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }

    // --- State extractor ---

    #[test]
    fn state_extractor_success() {
        #[derive(Clone, Debug, PartialEq)]
        struct AppConfig {
            name: String,
        }

        let cfg = AppConfig { name: "test".into() };
        let state = StateMapBuilder::new().insert(cfg.clone()).build();

        let mut req = get_request("/test");
        req.set_state(state);

        let State(extracted) = ok_or_panic(State::<AppConfig>::from_parts(&req));
        assert_eq!(extracted, cfg);
    }

    #[test]
    fn state_extractor_missing_returns_500() {
        let req = get_request("/test");
        let err = err_or_panic(State::<String>::from_parts(&req));
        assert_eq!(err.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    // --- Method extractor ---

    #[test]
    fn method_extractor() {
        let req = Request::new(
            Method::POST,
            "/test".parse().unwrap(),
            HeaderMap::new(),
            Bytes::new(),
        );
        let method = ok_or_panic(Method::from_parts(&req));
        assert_eq!(method, Method::POST);
    }

    // --- Uri extractor ---

    #[test]
    fn uri_extractor() {
        let req = get_request("/api/v1/users?page=1");
        let uri = ok_or_panic(<Uri as FromRequestParts>::from_parts(&req));
        assert_eq!(uri.path(), "/api/v1/users");
        assert_eq!(uri.query(), Some("page=1"));
    }

    // --- HeaderMap extractor ---

    #[test]
    fn headermap_extractor() {
        let mut headers = HeaderMap::new();
        headers.insert("x-custom", HeaderValue::from_static("hello"));
        let req = Request::new(
            Method::GET,
            "/test".parse().unwrap(),
            headers,
            Bytes::new(),
        );
        let extracted = ok_or_panic(HeaderMap::from_parts(&req));
        assert_eq!(extracted.get("x-custom").unwrap(), "hello");
    }

    // --- Extension extractor ---

    #[test]
    fn extension_extractor_success() {
        #[derive(Clone, Debug, PartialEq)]
        struct UserId(u64);

        let mut req = get_request("/test");
        req.set_extension(UserId(42));

        let Extension(user_id) = ok_or_panic(Extension::<UserId>::from_parts(&req));
        assert_eq!(user_id, UserId(42));
    }

    #[test]
    fn extension_extractor_missing_returns_500() {
        #[derive(Clone)]
        struct MissingType;

        let req = get_request("/test");
        let err = err_or_panic(Extension::<MissingType>::from_parts(&req));
        assert_eq!(err.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    // --- ConnectInfo extractor ---

    #[test]
    fn connect_info_extractor_success() {
        let mut req = get_request("/test");
        let addr: SocketAddr = "192.168.1.1:12345".parse().unwrap();
        req.set_remote_addr(addr);

        let ConnectInfo(extracted) = ok_or_panic(ConnectInfo::from_parts(&req));
        assert_eq!(extracted, addr);
    }

    #[test]
    fn connect_info_extractor_missing_returns_500() {
        let req = get_request("/test");
        let err = err_or_panic(ConnectInfo::from_parts(&req));
        assert_eq!(err.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    // --- Body extractors: Bytes, String ---

    #[test]
    fn bytes_extractor() {
        let body_data = Bytes::from("hello world");
        let req = Request::new(
            Method::POST,
            "/test".parse().unwrap(),
            HeaderMap::new(),
            body_data.clone(),
        );
        let extracted = ok_or_panic(Bytes::from_request(&req));
        assert_eq!(extracted, body_data);
    }

    #[test]
    fn string_extractor_valid_utf8() {
        let req = Request::new(
            Method::POST,
            "/test".parse().unwrap(),
            HeaderMap::new(),
            Bytes::from("hello"),
        );
        let extracted = ok_or_panic(String::from_request(&req));
        assert_eq!(extracted, "hello");
    }

    #[test]
    fn string_extractor_invalid_utf8_returns_400() {
        let req = Request::new(
            Method::POST,
            "/test".parse().unwrap(),
            HeaderMap::new(),
            Bytes::from_static(&[0xFF, 0xFE]),
        );
        let err = err_or_panic(String::from_request(&req));
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }

    // --- Json extractor ---

    #[cfg(feature = "json")]
    #[test]
    fn json_extractor_success() {
        use serde::Deserialize;

        #[derive(Deserialize, Debug, PartialEq)]
        struct User {
            name: String,
            age: u32,
        }

        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/json"));
        let body = Bytes::from(r#"{"name":"Alice","age":30}"#);

        let req = Request::new(Method::POST, "/test".parse().unwrap(), headers, body);
        let Json(user) = ok_or_panic(Json::<User>::from_request(&req));
        assert_eq!(user, User { name: "Alice".into(), age: 30 });
    }

    #[cfg(feature = "json")]
    #[test]
    fn json_extractor_wrong_content_type_returns_415() {
        use serde::Deserialize;

        #[derive(Deserialize)]
        struct User {
            #[allow(dead_code)]
            name: String,
        }

        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("text/plain"));
        let body = Bytes::from(r#"{"name":"Alice"}"#);

        let req = Request::new(Method::POST, "/test".parse().unwrap(), headers, body);
        let err = err_or_panic(Json::<User>::from_request(&req));
        assert_eq!(err.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    #[cfg(feature = "json")]
    #[test]
    fn json_extractor_invalid_json_returns_400() {
        use serde::Deserialize;

        #[derive(Deserialize)]
        struct User {
            #[allow(dead_code)]
            name: String,
        }

        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/json"));
        let body = Bytes::from("not json");

        let req = Request::new(Method::POST, "/test".parse().unwrap(), headers, body);
        let err = err_or_panic(Json::<User>::from_request(&req));
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }

    #[cfg(feature = "json")]
    #[test]
    fn json_extractor_no_content_type_returns_415() {
        use serde::Deserialize;

        #[derive(Deserialize)]
        struct User {
            #[allow(dead_code)]
            name: String,
        }

        let body = Bytes::from(r#"{"name":"Alice"}"#);
        let req = Request::new(Method::POST, "/test".parse().unwrap(), HeaderMap::new(), body);
        let err = err_or_panic(Json::<User>::from_request(&req));
        assert_eq!(err.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    // --- Form extractor ---

    #[cfg(feature = "form")]
    #[test]
    fn form_extractor_success() {
        use serde::Deserialize;

        #[derive(Deserialize, Debug, PartialEq)]
        struct Login {
            username: String,
            password: String,
        }

        let mut headers = HeaderMap::new();
        headers.insert(
            "content-type",
            HeaderValue::from_static("application/x-www-form-urlencoded"),
        );
        let body = Bytes::from("username=alice&password=secret");

        let req = Request::new(Method::POST, "/login".parse().unwrap(), headers, body);
        let Form(login) = ok_or_panic(Form::<Login>::from_request(&req));
        assert_eq!(login, Login { username: "alice".into(), password: "secret".into() });
    }

    #[cfg(feature = "form")]
    #[test]
    fn form_extractor_wrong_content_type_returns_415() {
        use serde::Deserialize;

        #[derive(Deserialize)]
        struct Login {
            #[allow(dead_code)]
            username: String,
        }

        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("text/plain"));
        let body = Bytes::from("username=alice");

        let req = Request::new(Method::POST, "/login".parse().unwrap(), headers, body);
        let err = err_or_panic(Form::<Login>::from_request(&req));
        assert_eq!(err.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    #[cfg(feature = "form")]
    #[test]
    fn form_extractor_invalid_data_returns_400() {
        use serde::Deserialize;

        #[derive(Deserialize)]
        struct Login {
            #[allow(dead_code)]
            username: String,
            #[allow(dead_code)]
            age: u32,
        }

        let mut headers = HeaderMap::new();
        headers.insert(
            "content-type",
            HeaderValue::from_static("application/x-www-form-urlencoded"),
        );
        let body = Bytes::from("username=alice&age=notanumber");

        let req = Request::new(Method::POST, "/login".parse().unwrap(), headers, body);
        let err = err_or_panic(Form::<Login>::from_request(&req));
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }

    // --- Optional extractor ---

    #[test]
    fn optional_extractor_present() {
        #[derive(Clone, Debug, PartialEq)]
        struct UserId(u64);

        let mut req = get_request("/test");
        req.set_extension(UserId(42));

        let result = ok_or_panic(Optional::<Extension<UserId>>::from_parts(&req));
        assert!(result.0.is_some());
        assert_eq!(result.0.unwrap().0, UserId(42));
    }

    #[test]
    fn optional_extractor_absent() {
        #[derive(Clone)]
        struct UserId(u64);

        let req = get_request("/test");
        let result = ok_or_panic(Optional::<Extension<UserId>>::from_parts(&req));
        assert!(result.0.is_none());
    }

    // --- TypedHeader extractor ---

    #[test]
    fn typed_header_content_type() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/json"));
        let req = Request::new(Method::GET, "/test".parse().unwrap(), headers, Bytes::new());

        let TypedHeader(ct) = ok_or_panic(TypedHeader::<ContentType>::from_parts(&req));
        assert_eq!(ct.0, "application/json");
    }

    #[test]
    fn typed_header_missing_returns_400() {
        let req = get_request("/test");
        let err = err_or_panic(TypedHeader::<ContentType>::from_parts(&req));
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn typed_header_authorization() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", HeaderValue::from_static("Basic abc123"));
        let req = Request::new(Method::GET, "/test".parse().unwrap(), headers, Bytes::new());

        let TypedHeader(auth) = ok_or_panic(TypedHeader::<Authorization>::from_parts(&req));
        assert_eq!(auth.0, "Basic abc123");
    }

    #[test]
    fn typed_header_accept() {
        let mut headers = HeaderMap::new();
        headers.insert("accept", HeaderValue::from_static("text/html"));
        let req = Request::new(Method::GET, "/test".parse().unwrap(), headers, Bytes::new());

        let TypedHeader(accept) = ok_or_panic(TypedHeader::<Accept>::from_parts(&req));
        assert_eq!(accept.0, "text/html");
    }

    #[test]
    fn typed_header_user_agent() {
        let mut headers = HeaderMap::new();
        headers.insert("user-agent", HeaderValue::from_static("Neutron/1.0"));
        let req = Request::new(Method::GET, "/test".parse().unwrap(), headers, Bytes::new());

        let TypedHeader(ua) = ok_or_panic(TypedHeader::<UserAgent>::from_parts(&req));
        assert_eq!(ua.0, "Neutron/1.0");
    }

    #[test]
    fn typed_header_host() {
        let mut headers = HeaderMap::new();
        headers.insert("host", HeaderValue::from_static("example.com"));
        let req = Request::new(Method::GET, "/test".parse().unwrap(), headers, Bytes::new());

        let TypedHeader(host) = ok_or_panic(TypedHeader::<Host>::from_parts(&req));
        assert_eq!(host.0, "example.com");
    }

    #[test]
    fn typed_header_origin() {
        let mut headers = HeaderMap::new();
        headers.insert("origin", HeaderValue::from_static("https://example.com"));
        let req = Request::new(Method::GET, "/test".parse().unwrap(), headers, Bytes::new());

        let TypedHeader(origin) = ok_or_panic(TypedHeader::<Origin>::from_parts(&req));
        assert_eq!(origin.0, "https://example.com");
    }

    // --- BearerToken ---

    #[test]
    fn bearer_token_success() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            HeaderValue::from_static("Bearer my-secret-token"),
        );
        let req = Request::new(Method::GET, "/test".parse().unwrap(), headers, Bytes::new());

        let TypedHeader(BearerToken(token)) =
            ok_or_panic(TypedHeader::<BearerToken>::from_parts(&req));
        assert_eq!(token, "my-secret-token");
    }

    #[test]
    fn bearer_token_wrong_scheme_returns_400() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            HeaderValue::from_static("Basic abc123"),
        );
        let req = Request::new(Method::GET, "/test".parse().unwrap(), headers, Bytes::new());

        let err = err_or_panic(TypedHeader::<BearerToken>::from_parts(&req));
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn bearer_token_missing_header_returns_400() {
        let req = get_request("/test");
        let err = err_or_panic(TypedHeader::<BearerToken>::from_parts(&req));
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }

    // --- Blanket FromRequest impl for FromRequestParts types ---

    #[test]
    fn from_request_parts_blanket_impl() {
        // Method implements FromRequestParts; via blanket, it also implements FromRequest.
        let req = Request::new(
            Method::DELETE,
            "/test".parse().unwrap(),
            HeaderMap::new(),
            Bytes::new(),
        );
        let method = ok_or_panic(<Method as FromRequest>::from_request(&req));
        assert_eq!(method, Method::DELETE);
    }
}
