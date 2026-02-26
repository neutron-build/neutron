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

    // Some providers (GitHub) return form-encoded instead of JSON
    let token = if resp_text.trim_start().starts_with('{') {
        serde_json::from_str::<TokenResponse>(&resp_text)
            .map_err(|e| OAuthError::TokenExchange(format!("JSON parse: {e}: {resp_text}")))?
    } else {
        parse_form_response(&resp_text)?
    };

    Ok(token)
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
}
