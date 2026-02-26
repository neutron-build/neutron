//! Error type for neutron-oauth.

use std::fmt;

/// All errors that can arise during an OAuth2 flow.
#[derive(Debug)]
pub enum OAuthError {
    /// URL parsing or construction failure.
    BadUrl(String),
    /// TCP or TLS connection failure to the provider.
    Connect(String),
    /// HTTP request failure during token exchange or userinfo fetch.
    Http(String),
    /// The token endpoint returned a non-200 status or unparseable body.
    TokenExchange(String),
    /// The `state` parameter is missing, wrong, or the cookie is invalid.
    InvalidState,
    /// The `code` query parameter is missing from the callback.
    MissingCode,
    /// Fetching or parsing the userinfo endpoint failed.
    UserInfo(String),
    /// The signed state cookie failed HMAC verification (possible CSRF).
    CsrfViolation,
}

impl fmt::Display for OAuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BadUrl(u)        => write!(f, "OAuth bad URL: {u}"),
            Self::Connect(e)       => write!(f, "OAuth connect: {e}"),
            Self::Http(e)          => write!(f, "OAuth HTTP: {e}"),
            Self::TokenExchange(e) => write!(f, "OAuth token exchange: {e}"),
            Self::InvalidState     => write!(f, "OAuth invalid state parameter"),
            Self::MissingCode      => write!(f, "OAuth missing 'code' parameter"),
            Self::UserInfo(e)      => write!(f, "OAuth userinfo: {e}"),
            Self::CsrfViolation    => write!(f, "OAuth CSRF violation — state mismatch"),
        }
    }
}

impl std::error::Error for OAuthError {}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_all_variants() {
        assert!(OAuthError::BadUrl("x".into()).to_string().contains('x'));
        assert!(OAuthError::Connect("c".into()).to_string().contains('c'));
        assert!(OAuthError::Http("h".into()).to_string().contains('h'));
        assert!(OAuthError::TokenExchange("t".into()).to_string().contains('t'));
        assert!(OAuthError::InvalidState.to_string().contains("invalid"));
        assert!(OAuthError::MissingCode.to_string().contains("missing"));
        assert!(OAuthError::UserInfo("u".into()).to_string().contains('u'));
        assert!(OAuthError::CsrfViolation.to_string().contains("CSRF"));
    }
}
