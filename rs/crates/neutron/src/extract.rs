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
