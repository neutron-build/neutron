//! OAuth2 provider configuration and built-in provider presets.

/// Configuration for an OAuth2 / OIDC provider.
///
/// Build manually or use a preset from [`OAuthProvider`]:
///
/// ```rust,ignore
/// let config = OAuthProvider::github()
///     .client_id("my_id")
///     .client_secret("my_secret")
///     .redirect_uri("https://myapp.com/auth/github/callback")
///     .secret(b"signing-key-32-bytes-or-more!!!!".to_vec());
/// ```
#[derive(Debug, Clone)]
pub struct OAuthConfig {
    pub client_id:     String,
    pub client_secret: String,
    /// Provider's authorization endpoint (browser redirect target).
    pub auth_url:      String,
    /// Provider's token endpoint (server-to-server code exchange).
    pub token_url:     String,
    /// Provider's userinfo endpoint (optional; some providers embed claims in the ID token).
    pub userinfo_url:  Option<String>,
    /// The registered redirect URI that the provider will call back on.
    pub redirect_uri:  String,
    /// Scopes to request (e.g. `["openid", "profile", "email"]`).
    pub scopes:        Vec<String>,
    /// Secret used to HMAC-sign the state cookie (at least 32 bytes recommended).
    pub(crate) secret: Vec<u8>,
}

impl OAuthConfig {
    pub fn new(
        auth_url:  impl Into<String>,
        token_url: impl Into<String>,
    ) -> Self {
        Self {
            client_id:    String::new(),
            client_secret: String::new(),
            auth_url:     auth_url.into(),
            token_url:    token_url.into(),
            userinfo_url: None,
            redirect_uri: String::new(),
            scopes:       Vec::new(),
            secret:       Vec::new(),
        }
    }

    pub fn client_id(mut self, id: impl Into<String>) -> Self {
        self.client_id = id.into();
        self
    }

    pub fn client_secret(mut self, secret: impl Into<String>) -> Self {
        self.client_secret = secret.into();
        self
    }

    pub fn redirect_uri(mut self, uri: impl Into<String>) -> Self {
        self.redirect_uri = uri.into();
        self
    }

    pub fn scope(mut self, s: impl Into<String>) -> Self {
        self.scopes.push(s.into());
        self
    }

    pub fn scopes(mut self, scopes: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.scopes.extend(scopes.into_iter().map(|s| s.into()));
        self
    }

    pub fn userinfo_url(mut self, url: impl Into<String>) -> Self {
        self.userinfo_url = Some(url.into());
        self
    }

    /// Set the secret used to HMAC-sign the anti-CSRF state cookie.
    pub fn secret(mut self, key: Vec<u8>) -> Self {
        self.secret = key;
        self
    }

    /// Build the authorization URL to redirect the browser to.
    pub(crate) fn authorization_url(
        &self,
        state:     &str,
        challenge: &str,
    ) -> String {
        let scopes = self.scopes.join(" ");
        let scope_enc  = url_encode(&scopes);
        let redir_enc  = url_encode(&self.redirect_uri);
        let state_enc  = url_encode(state);
        let chall_enc  = url_encode(challenge);
        let cid_enc    = url_encode(&self.client_id);

        format!(
            "{}?response_type=code\
             &client_id={cid_enc}\
             &redirect_uri={redir_enc}\
             &scope={scope_enc}\
             &state={state_enc}\
             &code_challenge={chall_enc}\
             &code_challenge_method=S256",
            self.auth_url,
        )
    }
}

// ---------------------------------------------------------------------------
// OAuthProvider — built-in presets
// ---------------------------------------------------------------------------

/// Factory methods that return pre-configured [`OAuthConfig`] for popular
/// providers.  Fill in `client_id`, `client_secret`, `redirect_uri`, and
/// `secret` before use.
pub struct OAuthProvider;

impl OAuthProvider {
    /// GitHub — default scopes: `read:user user:email`
    pub fn github() -> OAuthConfig {
        OAuthConfig::new(
            "https://github.com/login/oauth/authorize",
            "https://github.com/login/oauth/access_token",
        )
        .scopes(["read:user", "user:email"])
    }

    /// Google — default scopes: `openid profile email`
    pub fn google() -> OAuthConfig {
        OAuthConfig::new(
            "https://accounts.google.com/o/oauth2/v2/auth",
            "https://oauth2.googleapis.com/token",
        )
        .userinfo_url("https://openidconnect.googleapis.com/v1/userinfo")
        .scopes(["openid", "profile", "email"])
    }

    /// Discord — default scopes: `identify email`
    pub fn discord() -> OAuthConfig {
        OAuthConfig::new(
            "https://discord.com/api/oauth2/authorize",
            "https://discord.com/api/oauth2/token",
        )
        .userinfo_url("https://discord.com/api/users/@me")
        .scopes(["identify", "email"])
    }

    /// Generic — supply your own URLs and scopes.
    pub fn custom(auth_url: impl Into<String>, token_url: impl Into<String>) -> OAuthConfig {
        OAuthConfig::new(auth_url, token_url)
    }
}

// ---------------------------------------------------------------------------
// Minimal URL percent-encoding for query-string values
// ---------------------------------------------------------------------------

pub(crate) fn url_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9'
            | b'-' | b'_' | b'.' | b'~' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn github_preset_has_correct_auth_url() {
        let cfg = OAuthProvider::github();
        assert!(cfg.auth_url.contains("github.com"));
        assert!(cfg.scopes.contains(&"read:user".to_string()));
    }

    #[test]
    fn google_preset_has_userinfo_url() {
        let cfg = OAuthProvider::google();
        assert!(cfg.userinfo_url.is_some());
        assert!(cfg.scopes.contains(&"openid".to_string()));
    }

    #[test]
    fn discord_preset_scopes() {
        let cfg = OAuthProvider::discord();
        assert!(cfg.scopes.contains(&"identify".to_string()));
        assert!(cfg.scopes.contains(&"email".to_string()));
    }

    #[test]
    fn builder_chain() {
        let cfg = OAuthConfig::new("https://auth.example.com", "https://token.example.com")
            .client_id("my-id")
            .client_secret("my-secret")
            .redirect_uri("https://app.example.com/callback")
            .scope("read")
            .scope("write")
            .secret(b"supersecret".to_vec());

        assert_eq!(cfg.client_id, "my-id");
        assert_eq!(cfg.scopes, vec!["read", "write"]);
        assert!(!cfg.secret.is_empty());
    }

    #[test]
    fn authorization_url_contains_all_params() {
        let cfg = OAuthConfig::new("https://auth.example.com/oauth", "https://token.example.com")
            .client_id("cid")
            .redirect_uri("https://app.example.com/cb")
            .scope("openid");

        let url = cfg.authorization_url("mystate", "mychallenge");
        assert!(url.contains("response_type=code"));
        assert!(url.contains("client_id=cid"));
        assert!(url.contains("code_challenge=mychallenge"));
        assert!(url.contains("code_challenge_method=S256"));
        assert!(url.contains("state=mystate"));
    }

    #[test]
    fn url_encode_encodes_special_chars() {
        assert_eq!(url_encode("hello world"), "hello%20world");
        assert_eq!(url_encode("a+b=c&d"), "a%2Bb%3Dc%26d");
        assert_eq!(url_encode("https://x.com"), "https%3A%2F%2Fx.com");
    }

    #[test]
    fn url_encode_leaves_safe_chars() {
        let safe = "abcABC123-_.~";
        assert_eq!(url_encode(safe), safe);
    }
}
