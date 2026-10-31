//! OAuth2 handler factories.
//!
//! Mount `oauth_redirect_handler` on the "login" route and
//! `oauth_callback_handler` on the registered callback URI.
//!
//! # Example
//!
//! ```rust,ignore
//! use neutron::router::Router;
//! use neutron_oauth::{OAuthProvider, oauth_redirect_handler, oauth_callback_handler};
//!
//! let config = OAuthProvider::github()
//!     .client_id(std::env::var("GITHUB_CLIENT_ID").unwrap())
//!     .client_secret(std::env::var("GITHUB_CLIENT_SECRET").unwrap())
//!     .redirect_uri("https://myapp.com/auth/github/callback")
//!     .secret(b"at-least-32-bytes-of-secret-key!!".to_vec());
//!
//! let router = Router::new()
//!     .get("/auth/github",          oauth_redirect_handler(config.clone()))
//!     .get("/auth/github/callback", oauth_callback_handler(config, on_login));
//!
//! async fn on_login(user: OAuthUser, _req: Request) -> Response {
//!     format!("Welcome, {}!", user.name.unwrap_or(user.id)).into_response()
//! }
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use http::StatusCode;
use neutron::cookie::SetCookie;
use neutron::handler::{IntoResponse, Request, Response};

use crate::config::OAuthConfig;
use crate::error::OAuthError;
use crate::pkce::PkceChallenge;
use crate::state::{decode_state_cookie, encode_state_cookie, generate_state, OAUTH_STATE_COOKIE};
use crate::token::exchange_code;
use crate::user::{fetch_userinfo, OAuthUser};

// ---------------------------------------------------------------------------
// oauth_redirect_handler
// ---------------------------------------------------------------------------

/// Return a handler that starts the OAuth2 authorization code flow.
///
/// When a browser hits this route the handler:
/// 1. Generates a PKCE challenge and a random anti-CSRF state.
/// 2. Stores both in a signed `HttpOnly` cookie.
/// 3. Redirects the browser to the provider's authorization URL.
pub fn oauth_redirect_handler(
    config: OAuthConfig,
) -> impl Fn(Request) -> Pin<Box<dyn Future<Output = Response> + Send>> + Clone + Send + Sync + 'static
{
    let config = Arc::new(config);
    move |_req: Request| {
        let config = Arc::clone(&config);
        Box::pin(async move {
            let pkce = PkceChallenge::new();
            let state = generate_state();

            let cookie_val = encode_state_cookie(&state, &pkce.verifier, &config.secret);
            let auth_url = config.authorization_url(&state, &pkce.challenge);

            let cookie_hdr = SetCookie::new(OAUTH_STATE_COOKIE, cookie_val)
                .path("/")
                .http_only()
                .secure()
                .same_site(neutron::cookie::SameSite::Lax)
                .max_age(600) // 10 minutes to complete login
                .to_header_value();

            // Redirect to provider
            http::Response::builder()
                .status(StatusCode::FOUND)
                .header("location", &auth_url)
                .header("set-cookie", cookie_hdr)
                .body(neutron::handler::Body::empty())
                .unwrap()
        })
    }
}

// ---------------------------------------------------------------------------
// oauth_callback_handler
// ---------------------------------------------------------------------------

/// Return a handler that completes the OAuth2 authorization code flow.
///
/// On a successful exchange the `on_success` callback is called with the
/// normalized [`OAuthUser`] and the original request.  The callback controls
/// what response is returned (e.g. set a session cookie and redirect).
///
/// On failure, a `400 Bad Request` response is returned.
pub fn oauth_callback_handler<F, Fut>(
    config: OAuthConfig,
    on_success: F,
) -> impl Fn(Request) -> Pin<Box<dyn Future<Output = Response> + Send>> + Clone + Send + Sync + 'static
where
    F: Fn(OAuthUser, Request) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = Response> + Send + 'static,
{
    let config = Arc::new(config);
    let on_success = Arc::new(on_success);

    move |req: Request| {
        let config = Arc::clone(&config);
        let on_success = Arc::clone(&on_success);
        Box::pin(async move {
            match handle_callback(req, config, on_success).await {
                Ok(resp) => resp,
                Err(e) => {
                    tracing::warn!("OAuth callback error: {e}");
                    (StatusCode::BAD_REQUEST, e.to_string()).into_response()
                }
            }
        })
    }
}

async fn handle_callback<F, Fut>(
    req: Request,
    config: Arc<OAuthConfig>,
    on_success: Arc<F>,
) -> Result<Response, OAuthError>
where
    F: Fn(OAuthUser, Request) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Response> + Send + 'static,
{
    // 1. Extract `code` and `state` from the query string.
    let query = req.uri().query().unwrap_or("");
    let params = parse_query(query).map_err(OAuthError::Http)?;

    let code = params.get("code").ok_or(OAuthError::MissingCode)?;
    let state = params.get("state").ok_or(OAuthError::InvalidState)?;

    // 2. Read and verify the signed state cookie.
    let cookie_header = req
        .headers()
        .get("cookie")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    let cookie_val =
        find_cookie(cookie_header, OAUTH_STATE_COOKIE).ok_or(OAuthError::InvalidState)?;

    let (stored_state, verifier) =
        decode_state_cookie(&cookie_val, &config.secret).ok_or(OAuthError::CsrfViolation)?;

    // 3. Verify the anti-CSRF state matches.
    if !constant_time_eq(state.as_bytes(), stored_state.as_bytes()) {
        return Err(OAuthError::CsrfViolation);
    }

    // 4. Exchange the authorization code for tokens.
    let tokens = exchange_code(&config, code, &verifier).await?;

    // 5. Fetch user information.
    let user = fetch_userinfo(&config, &tokens).await?;

    // 6. Clear the state cookie and call the success handler.
    let clear_hdr = SetCookie::new(OAUTH_STATE_COOKIE, "")
        .path("/")
        .max_age(0)
        .to_header_value();

    let resp = on_success(user, req).await;

    // Append the clear-cookie header to whatever the callback returned.
    let (mut parts, body) = resp.into_parts();
    if let Ok(v) = clear_hdr.parse() {
        parts.headers.append("set-cookie", v);
    }
    Ok(http::Response::from_parts(parts, body))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Parse an `application/x-www-form-urlencoded` query string with proper
/// percent-decoding.
///
/// Percent escapes are decoded (a provider may legally return a code such as
/// `a%2Fb`); failing to decode made the later token exchange re-encode the
/// literal escape (`%252Fb`-style double encoding) so valid logins failed.
/// `+` decodes to a space per the form encoding rules. Malformed escapes and
/// duplicate fields are rejected: a duplicated `code`/`state` is ambiguous
/// input, not something to silently resolve by last-wins.
fn parse_query(query: &str) -> Result<std::collections::HashMap<String, String>, String> {
    let mut out = std::collections::HashMap::new();
    for pair in query.split('&').filter(|s| !s.is_empty()) {
        let mut it = pair.splitn(2, '=');
        let k = it.next().unwrap_or("");
        if k.is_empty() {
            continue;
        }
        let v = it.next().unwrap_or("");
        let key = percent_decode_strict(k)?;
        let val = percent_decode_strict(v)?;
        if out.insert(key, val).is_some() {
            return Err("duplicate query field".to_string());
        }
    }
    Ok(out)
}

/// Strict `x-www-form-urlencoded` component decoder: `%HH` must be two hex
/// digits; `+` is a space. Returns `None` on malformed input.
fn percent_decode_strict(s: &str) -> Result<String, String> {
    let bytes = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' => {
                if i + 2 >= bytes.len() {
                    return Err(format!("truncated percent escape at byte {i}"));
                }
                let hi = (bytes[i + 1] as char).to_digit(16);
                let lo = (bytes[i + 2] as char).to_digit(16);
                match (hi, lo) {
                    (Some(h), Some(l)) => out.push(((h << 4) | l) as u8),
                    _ => return Err(format!("invalid percent escape at byte {i}")),
                }
                i += 2;
            }
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8(out).map_err(|e| format!("decoded value is not valid UTF-8: {e}"))
}

fn find_cookie(header: &str, name: &str) -> Option<String> {
    for part in header.split(';') {
        let part = part.trim();
        if let Some(rest) = part.strip_prefix(name) {
            if let Some(val) = rest.strip_prefix('=') {
                return Some(val.to_string());
            }
        }
    }
    None
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b.iter())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_query_basic() {
        let p = parse_query("code=abc&state=xyz&foo=bar").unwrap();
        assert_eq!(p.get("code").map(String::as_str), Some("abc"));
        assert_eq!(p.get("state").map(String::as_str), Some("xyz"));
    }

    #[test]
    fn parse_query_empty() {
        let p = parse_query("").unwrap();
        assert!(p.is_empty());
    }

    /// Regression (RS-16): authorization codes are percent-encoded on the
    /// wire; a provider may legitimately return `code=a%2Fb`. The value must
    /// be decoded exactly once here so the token exchange re-encodes the real
    /// code (previously the literal escape was re-encoded to `a%252Fb`).
    #[test]
    fn parse_query_percent_decodes_code() {
        let p = parse_query("code=a%2Fb&state=x%2By").unwrap();
        assert_eq!(p.get("code").map(String::as_str), Some("a/b"));
        assert_eq!(p.get("state").map(String::as_str), Some("x+y"));
    }

    /// `+` is form-encoded space, and an escaped `=` stays inside the value.
    #[test]
    fn parse_query_plus_and_escaped_equals() {
        let p = parse_query("code=a+b%3Dc&state=ok").unwrap();
        assert_eq!(p.get("code").map(String::as_str), Some("a b=c"));
    }

    #[test]
    fn parse_query_rejects_invalid_escapes() {
        assert!(parse_query("code=a%2").is_err()); // truncated
        assert!(parse_query("code=a%zz").is_err()); // non-hex
        assert!(parse_query("code=%").is_err()); // lone percent
    }

    /// A duplicated `code` (or any duplicated field) is ambiguous callback
    /// input and must be refused, not resolved by last-wins.
    #[test]
    fn parse_query_rejects_duplicate_fields() {
        assert!(parse_query("code=abc&code=def&state=x").is_err());
        assert!(parse_query("code=abc&state=1&state=2").is_err());
    }

    #[test]
    fn find_cookie_found() {
        let header = "session=abc; __oauth_state=myval; other=x";
        assert_eq!(
            find_cookie(header, "__oauth_state"),
            Some("myval".to_string())
        );
    }

    #[test]
    fn find_cookie_not_found() {
        let header = "session=abc";
        assert!(find_cookie(header, "__oauth_state").is_none());
    }

    #[test]
    fn constant_time_eq_correct() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"ab", b"abc"));
    }
}
