//! Token exchange — trade the authorization code for access/refresh tokens.

use serde::Deserialize;

use crate::client::https_post;
use crate::config::{OAuthConfig, url_encode};
use crate::error::OAuthError;

// ---------------------------------------------------------------------------
// TokenResponse
// ---------------------------------------------------------------------------

/// The response from the provider's token endpoint.
#[derive(Debug, Clone, Deserialize)]
pub struct TokenResponse {
    pub access_token:  String,
    pub token_type:    String,
    pub expires_in:    Option<u64>,
    pub refresh_token: Option<String>,
    /// OIDC ID token (JWT).
    pub id_token:      Option<String>,
    pub scope:         Option<String>,
}

// ---------------------------------------------------------------------------
// Token exchange
// ---------------------------------------------------------------------------

/// Exchange an authorization `code` for tokens.
///
/// Sends a POST to `config.token_url` with the code, redirect URI, PKCE
/// verifier, and client credentials.
pub async fn exchange_code(
    config:        &OAuthConfig,
    code:          &str,
    code_verifier: &str,
) -> Result<TokenResponse, OAuthError> {
    let body = format!(
        "grant_type=authorization_code\
         &code={}\
         &redirect_uri={}\
         &client_id={}\
         &client_secret={}\
         &code_verifier={}",
        url_encode(code),
        url_encode(&config.redirect_uri),
        url_encode(&config.client_id),
        url_encode(&config.client_secret),
        url_encode(code_verifier),
    );

    let resp_text = https_post(&config.token_url, body).await?;
    parse_token_response(&resp_text)
}

/// Redeem a refresh token for a fresh access token.
///
/// Providers commonly omit `refresh_token` from the refresh response — Google
/// issues one only on initial consent — so when it is absent the caller's
/// existing token is carried forward rather than lost.
///
/// A revoked or expired grant surfaces as [`OAuthError::RefreshRejected`],
/// which callers should treat as "this account must re-authenticate" rather
/// than as a transient failure worth retrying.
pub async fn refresh_access_token(
    config:  &OAuthConfig,
    refresh: &str,
) -> Result<TokenResponse, OAuthError> {
    let body = format!(
        "grant_type=refresh_token\
         &refresh_token={}\
         &client_id={}\
         &client_secret={}",
        url_encode(refresh),
        url_encode(&config.client_id),
        url_encode(&config.client_secret),
    );

    let resp_text = https_post(&config.token_url, body).await?;
    let mut token = parse_token_response(&resp_text)?;

    if token.refresh_token.is_none() {
        token.refresh_token = Some(refresh.to_string());
    }

    Ok(token)
}

/// Parse a token endpoint response, in JSON or form-encoded form.
///
/// An RFC 6749 §5.2 error body is detected before parsing, so a revoked grant
/// reports itself as such instead of as a missing-field parse failure.
fn parse_token_response(resp_text: &str) -> Result<TokenResponse, OAuthError> {
    if let Some(err) = parse_oauth_error(resp_text) {
        return Err(err);
    }

    // Some providers (GitHub) return form-encoded instead of JSON
    if resp_text.trim_start().starts_with('{') {
        serde_json::from_str::<TokenResponse>(resp_text)
            .map_err(|e| OAuthError::TokenExchange(format!("JSON parse: {e}: {resp_text}")))
    } else {
        parse_form_response(resp_text)
    }
}

/// Recognise an RFC 6749 §5.2 error response in either encoding.
///
/// `invalid_grant` is the code every provider uses for a refresh token that
/// has been revoked, expired, or invalidated by a password change.
fn parse_oauth_error(resp_text: &str) -> Option<OAuthError> {
    let code = if resp_text.trim_start().starts_with('{') {
        let v: serde_json::Value = serde_json::from_str(resp_text).ok()?;
        v.get("error")?.as_str()?.to_string()
    } else {
        resp_text.split('&')
            .filter_map(|kv| kv.split_once('='))
            .find(|(k, _)| *k == "error")
            .map(|(_, v)| percent_decode(v))?
    };

    Some(match code.as_str() {
        "invalid_grant" => OAuthError::RefreshRejected(resp_text.to_string()),
        _               => OAuthError::TokenExchange(format!("{code}: {resp_text}")),
    })
}

/// Parse an `application/x-www-form-urlencoded` token response (GitHub style).
fn parse_form_response(body: &str) -> Result<TokenResponse, OAuthError> {
    let pairs: Vec<(&str, &str)> = body.split('&')
        .filter_map(|kv| {
            let (k, v) = kv.split_once('=')?;
            
            
            Some((k, v))
        })
        .collect();

    let get = |key: &str| -> Option<String> {
        pairs.iter()
            .find(|(k, _)| *k == key)
            .map(|(_, v)| percent_decode(v))
    };

    let access_token = get("access_token")
        .ok_or_else(|| OAuthError::TokenExchange("missing access_token".into()))?;

    Ok(TokenResponse {
        access_token,
        token_type:    get("token_type").unwrap_or_else(|| "bearer".into()),
        expires_in:    get("expires_in").and_then(|v| v.parse().ok()),
        refresh_token: get("refresh_token"),
        id_token:      get("id_token"),
        scope:         get("scope"),
    })
}

fn percent_decode(s: &str) -> String {
    let mut bytes = Vec::with_capacity(s.len());
    let mut chars = s.bytes().peekable();
    while let Some(b) = chars.next() {
        if b == b'%' {
            let hi = chars.next().map(hex_val).unwrap_or(0);
            let lo = chars.next().map(hex_val).unwrap_or(0);
            bytes.push(hi << 4 | lo);
        } else if b == b'+' {
            bytes.push(b' ');
        } else {
            bytes.push(b);
        }
    }
    String::from_utf8_lossy(&bytes).into_owned()
}

fn hex_val(b: u8) -> u8 {
    match b {
        b'0'..=b'9' => b - b'0',
        b'a'..=b'f' => b - b'a' + 10,
        b'A'..=b'F' => b - b'A' + 10,
        _           => 0,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_json_token_response() {
        let json = r#"{
            "access_token": "tok123",
            "token_type":   "Bearer",
            "expires_in":   3600,
            "refresh_token": "ref456",
            "scope": "read write"
        }"#;
        let tr: TokenResponse = serde_json::from_str(json).unwrap();
        assert_eq!(tr.access_token, "tok123");
        assert_eq!(tr.expires_in, Some(3600));
        assert_eq!(tr.refresh_token.as_deref(), Some("ref456"));
    }

    #[test]
    fn parse_form_encoded_response() {
        let body = "access_token=gho_test&token_type=bearer&scope=user%3Aemail";
        let tr = parse_form_response(body).unwrap();
        assert_eq!(tr.access_token, "gho_test");
        assert_eq!(tr.token_type, "bearer");
        assert_eq!(tr.scope.as_deref(), Some("user:email"));
    }

    #[test]
    fn parse_form_missing_access_token_errors() {
        let body = "token_type=bearer";
        assert!(parse_form_response(body).is_err());
    }

    #[test]
    fn percent_decode_basic() {
        assert_eq!(percent_decode("hello%20world"), "hello world");
        assert_eq!(percent_decode("user%3Aemail"), "user:email");
        assert_eq!(percent_decode("a+b"), "a b");
    }

    #[test]
    fn percent_decode_passthrough() {
        assert_eq!(percent_decode("unchanged"), "unchanged");
    }

    #[test]
    fn invalid_grant_reports_refresh_rejected() {
        let body = r#"{"error":"invalid_grant","error_description":"Token has been expired or revoked."}"#;
        match parse_token_response(body) {
            Err(OAuthError::RefreshRejected(msg)) => assert!(msg.contains("revoked")),
            other => panic!("expected RefreshRejected, got {other:?}"),
        }
    }

    #[test]
    fn form_encoded_error_is_detected() {
        let body = "error=bad_verification_code&error_description=expired";
        match parse_token_response(body) {
            Err(OAuthError::TokenExchange(msg)) => assert!(msg.contains("bad_verification_code")),
            other => panic!("expected TokenExchange, got {other:?}"),
        }
    }

    #[test]
    fn successful_response_is_not_mistaken_for_an_error() {
        let body = r#"{"access_token":"tok","token_type":"Bearer","expires_in":3599}"#;
        let tr = parse_token_response(body).unwrap();
        assert_eq!(tr.access_token, "tok");
        assert!(tr.refresh_token.is_none());
    }

    // Google omits refresh_token on refresh; losing it would strand the account.
    #[test]
    fn absent_refresh_token_is_carried_forward() {
        let body = r#"{"access_token":"new","token_type":"Bearer","expires_in":3599}"#;
        let mut token = parse_token_response(body).unwrap();
        assert!(token.refresh_token.is_none());

        if token.refresh_token.is_none() {
            token.refresh_token = Some("original".to_string());
        }
        assert_eq!(token.refresh_token.as_deref(), Some("original"));
    }

    #[test]
    fn rotated_refresh_token_replaces_the_old_one() {
        let body = r#"{"access_token":"new","token_type":"Bearer","refresh_token":"rotated"}"#;
        let token = parse_token_response(body).unwrap();
        assert_eq!(token.refresh_token.as_deref(), Some("rotated"));
    }
}
