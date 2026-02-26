//! Cookie handling: plain, signed (HMAC-SHA256), and encrypted (AES-256-GCM).
//!
//! Provides [`CookieJar`], [`SignedCookieJar`], and [`PrivateCookieJar`]
//! extractors, plus [`SetCookie`] for building `Set-Cookie` response headers.
//!
//! ```rust,ignore
//! async fn handler(cookies: CookieJar) -> String {
//!     cookies.get("session").unwrap_or("none").to_string()
//! }
//! ```

use std::fmt;

use crate::extract::FromRequest;
use crate::handler::{IntoResponse, Request, Response};
use http::StatusCode;

// ---------------------------------------------------------------------------
// SameSite
// ---------------------------------------------------------------------------

/// SameSite cookie attribute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SameSite {
    Strict,
    Lax,
    None,
}

impl fmt::Display for SameSite {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SameSite::Strict => write!(f, "Strict"),
            SameSite::Lax => write!(f, "Lax"),
            SameSite::None => write!(f, "None"),
        }
    }
}

// ---------------------------------------------------------------------------
// SetCookie
// ---------------------------------------------------------------------------

/// A cookie builder for `Set-Cookie` response headers.
///
/// # Example
///
/// ```rust,ignore
/// use neutron::prelude::*;
///
/// async fn login() -> SetCookie {
///     SetCookie::new("session", "abc123")
///         .path("/")
///         .http_only()
///         .secure()
///         .same_site(SameSite::Lax)
///         .max_age(3600)
/// }
/// ```
#[derive(Debug, Clone)]
pub struct SetCookie {
    name: String,
    value: String,
    path: Option<String>,
    domain: Option<String>,
    max_age: Option<i64>,
    secure: bool,
    http_only: bool,
    same_site: Option<SameSite>,
}

impl SetCookie {
    /// Create a new cookie with the given name and value.
    pub fn new(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
            path: None,
            domain: None,
            max_age: None,
            secure: false,
            http_only: false,
            same_site: None,
        }
    }

    /// Set the cookie path.
    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.path = Some(path.into());
        self
    }

    /// Set the cookie domain.
    pub fn domain(mut self, domain: impl Into<String>) -> Self {
        self.domain = Some(domain.into());
        self
    }

    /// Set the cookie max-age in seconds. Negative values delete the cookie.
    pub fn max_age(mut self, seconds: i64) -> Self {
        self.max_age = Some(seconds);
        self
    }

    /// Mark the cookie as Secure (HTTPS only).
    pub fn secure(mut self) -> Self {
        self.secure = true;
        self
    }

    /// Mark the cookie as HttpOnly (not accessible via JavaScript).
    pub fn http_only(mut self) -> Self {
        self.http_only = true;
        self
    }

    /// Set the SameSite attribute.
    pub fn same_site(mut self, same_site: SameSite) -> Self {
        self.same_site = Some(same_site);
        self
    }

    /// Build the Set-Cookie header value string.
    pub fn to_header_value(&self) -> String {
        let mut parts = vec![format!("{}={}", self.name, self.value)];

        if let Some(ref path) = self.path {
            parts.push(format!("Path={path}"));
        }
        if let Some(ref domain) = self.domain {
            parts.push(format!("Domain={domain}"));
        }
        if let Some(max_age) = self.max_age {
            parts.push(format!("Max-Age={max_age}"));
        }
        if self.secure {
            parts.push("Secure".to_string());
        }
        if self.http_only {
            parts.push("HttpOnly".to_string());
        }
        if let Some(ref same_site) = self.same_site {
            parts.push(format!("SameSite={same_site}"));
        }

        parts.join("; ")
    }

    /// Create a removal cookie (Max-Age=0) for the given name.
    pub fn remove(name: impl Into<String>) -> Self {
        Self::new(name, "").max_age(0)
    }
}

impl IntoResponse for SetCookie {
    fn into_response(self) -> Response {
        http::Response::builder()
            .status(StatusCode::OK)
            .header("set-cookie", self.to_header_value())
            .body(crate::handler::Body::empty())
            .unwrap()
    }
}

// ---------------------------------------------------------------------------
// Cookie parsing helper
// ---------------------------------------------------------------------------

fn parse_cookies_from_headers(headers: &http::HeaderMap) -> Vec<(String, String)> {
    headers
        .get_all("cookie")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|s| s.split(';'))
        .filter_map(|pair| {
            let pair = pair.trim();
            let (name, value) = pair.split_once('=')?;
            Some((name.trim().to_string(), value.trim().to_string()))
        })
        .collect()
}

// ---------------------------------------------------------------------------
// CookieJar (plain — no cryptography)
// ---------------------------------------------------------------------------

/// A collection of cookies parsed from the request `Cookie` header.
///
/// # Example
///
/// ```rust,ignore
/// use neutron::prelude::*;
///
/// async fn handler(cookies: CookieJar) -> String {
///     match cookies.get("session") {
///         Some(session) => format!("Session: {session}"),
///         None => "No session".to_string(),
///     }
/// }
/// ```
#[derive(Debug, Clone)]
pub struct CookieJar {
    cookies: Vec<(String, String)>,
}

impl CookieJar {
    /// Get a cookie value by name.
    pub fn get(&self, name: &str) -> Option<&str> {
        self.cookies
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }

    /// Check if a cookie exists.
    pub fn has(&self, name: &str) -> bool {
        self.cookies.iter().any(|(n, _)| n == name)
    }

    /// Get all cookies as (name, value) pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.cookies.iter().map(|(n, v)| (n.as_str(), v.as_str()))
    }

    /// Get the number of cookies.
    pub fn len(&self) -> usize {
        self.cookies.len()
    }

    /// Check if the jar is empty.
    pub fn is_empty(&self) -> bool {
        self.cookies.is_empty()
    }
}

impl FromRequest for CookieJar {
    fn from_request(req: &Request) -> Result<Self, Response> {
        Ok(CookieJar {
            cookies: parse_cookies_from_headers(req.headers()),
        })
    }
}

// ===========================================================================
// Cryptographic cookie support
// ===========================================================================

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use base64::{engine::general_purpose::STANDARD, Engine};
use hmac::Hmac;
use hmac::Mac;
use rand::RngCore;
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Length of base64-encoded HMAC-SHA256 digest (32 bytes → 44 base64 chars).
const BASE64_DIGEST_LEN: usize = 44;

/// AES-GCM nonce length in bytes.
const NONCE_LEN: usize = 12;

/// AES-GCM authentication tag length in bytes.
const TAG_LEN: usize = 16;

// ---------------------------------------------------------------------------
// Key
// ---------------------------------------------------------------------------

/// Cryptographic key for cookie signing and encryption.
///
/// Contains a 32-byte signing key (HMAC-SHA256) and a 32-byte encryption
/// key (AES-256-GCM). Store as application state to use with
/// [`SignedCookieJar`] and [`PrivateCookieJar`].
///
/// # Example
///
/// ```rust,ignore
/// use neutron::prelude::*;
/// use neutron::cookie::Key;
///
/// let key = Key::generate();
///
/// let router = Router::new()
///     .state(key)
///     .get("/read", |jar: SignedCookieJar| async move {
///         jar.get("session").unwrap_or("none").to_string()
///     })
///     .post("/login", |key: State<Key>| async move {
///         key.signed_cookie("session", "user123")
///             .path("/")
///             .http_only()
///     });
/// ```
#[derive(Clone)]
pub struct Key {
    signing: [u8; 32],
    encryption: [u8; 32],
}

impl fmt::Debug for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Key([REDACTED])")
    }
}

impl Key {
    /// Generate a new random key.
    pub fn generate() -> Self {
        let mut signing = [0u8; 32];
        let mut encryption = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut signing);
        rand::thread_rng().fill_bytes(&mut encryption);
        Self { signing, encryption }
    }

    /// Create a key from raw bytes (must be at least 64 bytes).
    ///
    /// Bytes 0..32 are used for signing, bytes 32..64 for encryption.
    ///
    /// # Panics
    ///
    /// Panics if `bytes` is shorter than 64 bytes.
    pub fn from_bytes(bytes: &[u8]) -> Self {
        assert!(
            bytes.len() >= 64,
            "Key must be at least 64 bytes, got {}",
            bytes.len()
        );
        let mut signing = [0u8; 32];
        let mut encryption = [0u8; 32];
        signing.copy_from_slice(&bytes[..32]);
        encryption.copy_from_slice(&bytes[32..64]);
        Self { signing, encryption }
    }

    /// Sign a cookie value with HMAC-SHA256.
    ///
    /// Returns a string with the base64-encoded signature prepended to the
    /// original value. Use this when creating signed `Set-Cookie` headers.
    pub fn sign(&self, value: &str) -> String {
        let mut mac = <HmacSha256 as Mac>::new_from_slice(&self.signing).unwrap();
        mac.update(value.as_bytes());
        let signature = STANDARD.encode(mac.finalize().into_bytes());
        format!("{signature}{value}")
    }

    /// Verify a signed cookie value and return the original plaintext.
    ///
    /// Returns `None` if the signature is invalid or the value is too short.
    pub fn verify(&self, signed_value: &str) -> Option<String> {
        if signed_value.len() < BASE64_DIGEST_LEN {
            return None;
        }
        let (digest_b64, value) = signed_value.split_at(BASE64_DIGEST_LEN);
        let digest = STANDARD.decode(digest_b64).ok()?;

        let mut mac = <HmacSha256 as Mac>::new_from_slice(&self.signing).unwrap();
        mac.update(value.as_bytes());
        mac.verify_slice(&digest).ok()?; // constant-time comparison

        Some(value.to_string())
    }

    /// Encrypt a cookie value with AES-256-GCM.
    ///
    /// The cookie name is used as associated authenticated data (AAD),
    /// binding the ciphertext to a specific cookie name.
    /// Returns a base64-encoded string of `nonce || ciphertext || tag`.
    pub fn encrypt(&self, name: &str, value: &str) -> String {
        let cipher = Aes256Gcm::new_from_slice(&self.encryption).unwrap();
        let mut nonce_bytes = [0u8; NONCE_LEN];
        rand::thread_rng().fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let payload = aes_gcm::aead::Payload {
            msg: value.as_bytes(),
            aad: name.as_bytes(),
        };
        let ciphertext = cipher.encrypt(nonce, payload).expect("encryption failed");

        let mut result = Vec::with_capacity(NONCE_LEN + ciphertext.len());
        result.extend_from_slice(&nonce_bytes);
        result.extend_from_slice(&ciphertext);
        STANDARD.encode(&result)
    }

    /// Decrypt a cookie value encrypted with AES-256-GCM.
    ///
    /// The cookie name must match the name used during encryption (AAD).
    /// Returns `None` if decryption fails (wrong key, tampered data, wrong name).
    pub fn decrypt(&self, name: &str, encrypted_value: &str) -> Option<String> {
        let data = STANDARD.decode(encrypted_value).ok()?;
        if data.len() < NONCE_LEN + TAG_LEN {
            return None;
        }

        let (nonce_bytes, ciphertext) = data.split_at(NONCE_LEN);
        let cipher = Aes256Gcm::new_from_slice(&self.encryption).unwrap();
        let nonce = Nonce::from_slice(nonce_bytes);

        let payload = aes_gcm::aead::Payload {
            msg: ciphertext,
            aad: name.as_bytes(),
        };
        let plaintext = cipher.decrypt(nonce, payload).ok()?;
        String::from_utf8(plaintext).ok()
    }

    /// Create a [`SetCookie`] with a signed value.
    ///
    /// ```rust,ignore
    /// key.signed_cookie("session", "user123")
    ///     .path("/")
    ///     .http_only()
    /// ```
    pub fn signed_cookie(&self, name: impl Into<String>, value: &str) -> SetCookie {
        let name = name.into();
        let signed = self.sign(value);
        SetCookie::new(name, signed)
    }

    /// Create a [`SetCookie`] with an encrypted value.
    ///
    /// ```rust,ignore
    /// key.private_cookie("secret", "sensitive-data")
    ///     .path("/")
    ///     .http_only()
    ///     .secure()
    /// ```
    pub fn private_cookie(&self, name: impl Into<String>, value: &str) -> SetCookie {
        let name = name.into();
        let encrypted = self.encrypt(&name, value);
        SetCookie::new(name, encrypted)
    }
}

// ---------------------------------------------------------------------------
// SignedCookieJar
// ---------------------------------------------------------------------------

/// A cookie jar that verifies HMAC-SHA256 signatures on cookie values.
///
/// Only cookies whose signatures are valid (signed with the same [`Key`])
/// are included. Unsigned or tampered cookies are silently ignored.
///
/// Requires [`Key`] to be registered as application state.
///
/// # Example
///
/// ```rust,ignore
/// use neutron::prelude::*;
/// use neutron::cookie::{Key, SignedCookieJar};
///
/// async fn handler(jar: SignedCookieJar) -> String {
///     jar.get("session").unwrap_or("none").to_string()
/// }
/// ```
#[derive(Debug, Clone)]
pub struct SignedCookieJar {
    cookies: Vec<(String, String)>,
}

impl SignedCookieJar {
    /// Get a verified cookie value by name.
    pub fn get(&self, name: &str) -> Option<&str> {
        self.cookies
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }

    /// Check if a verified cookie exists.
    pub fn has(&self, name: &str) -> bool {
        self.cookies.iter().any(|(n, _)| n == name)
    }

    /// Get all verified cookies as (name, value) pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.cookies.iter().map(|(n, v)| (n.as_str(), v.as_str()))
    }

    /// Get the number of verified cookies.
    pub fn len(&self) -> usize {
        self.cookies.len()
    }

    /// Check if the jar is empty.
    pub fn is_empty(&self) -> bool {
        self.cookies.is_empty()
    }
}

impl FromRequest for SignedCookieJar {
    fn from_request(req: &Request) -> Result<Self, Response> {
        let key = req.get_state::<Key>().ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Key not configured for SignedCookieJar",
            )
                .into_response()
        })?;

        let cookies = parse_cookies_from_headers(req.headers())
            .into_iter()
            .filter_map(|(name, value)| {
                let verified = key.verify(&value)?;
                Some((name, verified))
            })
            .collect();

        Ok(SignedCookieJar { cookies })
    }
}

// ---------------------------------------------------------------------------
// PrivateCookieJar
// ---------------------------------------------------------------------------

/// A cookie jar that decrypts AES-256-GCM encrypted cookie values.
///
/// Only cookies that decrypt successfully (encrypted with the same [`Key`])
/// are included. Non-encrypted or tampered cookies are silently ignored.
///
/// Requires [`Key`] to be registered as application state.
///
/// # Example
///
/// ```rust,ignore
/// use neutron::prelude::*;
/// use neutron::cookie::{Key, PrivateCookieJar};
///
/// async fn handler(jar: PrivateCookieJar) -> String {
///     jar.get("secret").unwrap_or("none").to_string()
/// }
/// ```
#[derive(Debug, Clone)]
pub struct PrivateCookieJar {
    cookies: Vec<(String, String)>,
}

impl PrivateCookieJar {
    /// Get a decrypted cookie value by name.
    pub fn get(&self, name: &str) -> Option<&str> {
        self.cookies
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }

    /// Check if a decrypted cookie exists.
    pub fn has(&self, name: &str) -> bool {
        self.cookies.iter().any(|(n, _)| n == name)
    }

    /// Get all decrypted cookies as (name, value) pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.cookies.iter().map(|(n, v)| (n.as_str(), v.as_str()))
    }

    /// Get the number of decrypted cookies.
    pub fn len(&self) -> usize {
        self.cookies.len()
    }

    /// Check if the jar is empty.
    pub fn is_empty(&self) -> bool {
        self.cookies.is_empty()
    }
}

impl FromRequest for PrivateCookieJar {
    fn from_request(req: &Request) -> Result<Self, Response> {
        let key = req.get_state::<Key>().ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Key not configured for PrivateCookieJar",
            )
                .into_response()
        })?;

        let cookies = parse_cookies_from_headers(req.headers())
            .into_iter()
            .filter_map(|(name, value)| {
                let decrypted = key.decrypt(&name, &value)?;
                Some((name, decrypted))
            })
            .collect();

        Ok(PrivateCookieJar { cookies })
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler::Json;
    use crate::router::Router;
    use crate::testing::TestClient;

    // -----------------------------------------------------------------------
    // Plain CookieJar tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn parse_single_cookie() {
        let client = TestClient::new(Router::new().get("/", |jar: CookieJar| async move {
            jar.get("session").unwrap_or("none").to_string()
        }));

        let resp = client
            .get("/")
            .header("cookie", "session=abc123")
            .send()
            .await;

        assert_eq!(resp.text().await, "abc123");
    }

    #[tokio::test]
    async fn parse_multiple_cookies() {
        let client = TestClient::new(Router::new().get("/", |jar: CookieJar| async move {
            Json(serde_json::json!({
                "a": jar.get("a"),
                "b": jar.get("b"),
                "c": jar.get("c"),
            }))
        }));

        let resp = client
            .get("/")
            .header("cookie", "a=1; b=2; c=3")
            .send()
            .await;

        let body: serde_json::Value = resp.json().await;
        assert_eq!(body["a"], "1");
        assert_eq!(body["b"], "2");
        assert_eq!(body["c"], "3");
    }

    #[tokio::test]
    async fn missing_cookie_returns_none() {
        let client = TestClient::new(Router::new().get("/", |jar: CookieJar| async move {
            jar.get("missing").unwrap_or("none").to_string()
        }));

        let resp = client.get("/").send().await;
        assert_eq!(resp.text().await, "none");
    }

    #[tokio::test]
    async fn empty_cookie_header() {
        let client = TestClient::new(Router::new().get("/", |jar: CookieJar| async move {
            format!("{}", jar.len())
        }));

        let resp = client.get("/").send().await;
        assert_eq!(resp.text().await, "0");
    }

    #[tokio::test]
    async fn cookie_jar_has() {
        let client = TestClient::new(Router::new().get("/", |jar: CookieJar| async move {
            Json(serde_json::json!({
                "has_token": jar.has("token"),
                "has_missing": jar.has("missing"),
            }))
        }));

        let resp = client
            .get("/")
            .header("cookie", "token=xyz")
            .send()
            .await;

        let body: serde_json::Value = resp.json().await;
        assert_eq!(body["has_token"], true);
        assert_eq!(body["has_missing"], false);
    }

    #[tokio::test]
    async fn set_cookie_response() {
        let client = TestClient::new(Router::new().get("/login", || async {
            SetCookie::new("session", "abc123")
                .path("/")
                .http_only()
                .secure()
                .same_site(SameSite::Lax)
                .max_age(3600)
        }));

        let resp = client.get("/login").send().await;
        assert_eq!(resp.status(), StatusCode::OK);

        let set_cookie = resp.header("set-cookie").unwrap().to_string();
        assert!(set_cookie.contains("session=abc123"));
        assert!(set_cookie.contains("Path=/"));
        assert!(set_cookie.contains("HttpOnly"));
        assert!(set_cookie.contains("Secure"));
        assert!(set_cookie.contains("SameSite=Lax"));
        assert!(set_cookie.contains("Max-Age=3600"));
    }

    #[tokio::test]
    async fn set_cookie_minimal() {
        let cookie = SetCookie::new("theme", "dark");
        assert_eq!(cookie.to_header_value(), "theme=dark");
    }

    #[tokio::test]
    async fn set_cookie_with_domain() {
        let cookie = SetCookie::new("id", "42").domain("example.com");
        let val = cookie.to_header_value();
        assert!(val.contains("Domain=example.com"));
    }

    #[tokio::test]
    async fn remove_cookie() {
        let cookie = SetCookie::remove("session");
        let val = cookie.to_header_value();
        assert!(val.contains("session="));
        assert!(val.contains("Max-Age=0"));
    }

    #[tokio::test]
    async fn same_site_strict() {
        let cookie = SetCookie::new("x", "1").same_site(SameSite::Strict);
        assert!(cookie.to_header_value().contains("SameSite=Strict"));
    }

    #[tokio::test]
    async fn same_site_none_requires_secure() {
        let cookie = SetCookie::new("x", "1")
            .same_site(SameSite::None)
            .secure();
        let val = cookie.to_header_value();
        assert!(val.contains("SameSite=None"));
        assert!(val.contains("Secure"));
    }

    #[tokio::test]
    async fn cookie_value_with_equals() {
        // Cookie values can contain = (e.g., base64)
        let client = TestClient::new(Router::new().get("/", |jar: CookieJar| async move {
            jar.get("token").unwrap_or("none").to_string()
        }));

        let resp = client
            .get("/")
            .header("cookie", "token=abc=def==")
            .send()
            .await;

        assert_eq!(resp.text().await, "abc=def==");
    }

    #[tokio::test]
    async fn cookie_jar_iter() {
        let client = TestClient::new(Router::new().get("/", |jar: CookieJar| async move {
            let count = jar.iter().count();
            format!("{count}")
        }));

        let resp = client
            .get("/")
            .header("cookie", "a=1; b=2; c=3")
            .send()
            .await;

        assert_eq!(resp.text().await, "3");
    }

    // -----------------------------------------------------------------------
    // Key tests
    // -----------------------------------------------------------------------

    #[test]
    fn key_generate_unique() {
        let k1 = Key::generate();
        let k2 = Key::generate();
        assert_ne!(k1.signing, k2.signing);
        assert_ne!(k1.encryption, k2.encryption);
    }

    #[test]
    fn key_from_bytes() {
        let bytes = [42u8; 64];
        let key = Key::from_bytes(&bytes);
        assert_eq!(key.signing, [42u8; 32]);
        assert_eq!(key.encryption, [42u8; 32]);
    }

    #[test]
    #[should_panic(expected = "Key must be at least 64 bytes")]
    fn key_from_bytes_too_short() {
        Key::from_bytes(&[0u8; 32]);
    }

    #[test]
    fn key_debug_redacted() {
        let key = Key::generate();
        let debug = format!("{key:?}");
        assert_eq!(debug, "Key([REDACTED])");
        assert!(!debug.contains("signing"));
    }

    // -----------------------------------------------------------------------
    // Signing tests
    // -----------------------------------------------------------------------

    #[test]
    fn sign_and_verify_roundtrip() {
        let key = Key::generate();
        let signed = key.sign("hello world");
        let verified = key.verify(&signed);
        assert_eq!(verified.as_deref(), Some("hello world"));
    }

    #[test]
    fn sign_produces_deterministic_signature() {
        let key = Key::from_bytes(&[1u8; 64]);
        let s1 = key.sign("test");
        let s2 = key.sign("test");
        assert_eq!(s1, s2);
    }

    #[test]
    fn verify_rejects_tampered_value() {
        let key = Key::generate();
        let mut signed = key.sign("hello");
        // Tamper with the value part (after the 44-char signature)
        signed.push('X');
        assert!(key.verify(&signed).is_none());
    }

    #[test]
    fn verify_rejects_tampered_signature() {
        let key = Key::generate();
        let signed = key.sign("hello");
        // Tamper with the signature (flip first char)
        let mut chars: Vec<char> = signed.chars().collect();
        chars[0] = if chars[0] == 'A' { 'B' } else { 'A' };
        let tampered: String = chars.into_iter().collect();
        assert!(key.verify(&tampered).is_none());
    }

    #[test]
    fn verify_rejects_wrong_key() {
        let k1 = Key::generate();
        let k2 = Key::generate();
        let signed = k1.sign("secret");
        assert!(k2.verify(&signed).is_none());
    }

    #[test]
    fn verify_rejects_empty() {
        let key = Key::generate();
        assert!(key.verify("").is_none());
    }

    #[test]
    fn verify_rejects_short_value() {
        let key = Key::generate();
        assert!(key.verify("tooshort").is_none());
    }

    #[test]
    fn sign_empty_value() {
        let key = Key::generate();
        let signed = key.sign("");
        let verified = key.verify(&signed);
        assert_eq!(verified.as_deref(), Some(""));
    }

    // -----------------------------------------------------------------------
    // Encryption tests
    // -----------------------------------------------------------------------

    #[test]
    fn encrypt_and_decrypt_roundtrip() {
        let key = Key::generate();
        let encrypted = key.encrypt("session", "sensitive data");
        let decrypted = key.decrypt("session", &encrypted);
        assert_eq!(decrypted.as_deref(), Some("sensitive data"));
    }

    #[test]
    fn encrypt_produces_different_ciphertexts() {
        let key = Key::generate();
        let e1 = key.encrypt("cookie", "same value");
        let e2 = key.encrypt("cookie", "same value");
        // Different nonces should produce different ciphertexts
        assert_ne!(e1, e2);
    }

    #[test]
    fn decrypt_rejects_wrong_name() {
        let key = Key::generate();
        let encrypted = key.encrypt("session", "value");
        // Decrypting with different name (AAD mismatch) should fail
        assert!(key.decrypt("other", &encrypted).is_none());
    }

    #[test]
    fn decrypt_rejects_wrong_key() {
        let k1 = Key::generate();
        let k2 = Key::generate();
        let encrypted = k1.encrypt("session", "value");
        assert!(k2.decrypt("session", &encrypted).is_none());
    }

    #[test]
    fn decrypt_rejects_tampered_data() {
        let key = Key::generate();
        let encrypted = key.encrypt("session", "value");
        let mut bytes = STANDARD.decode(&encrypted).unwrap();
        // Flip a byte in the ciphertext
        if let Some(b) = bytes.last_mut() {
            *b ^= 0xFF;
        }
        let tampered = STANDARD.encode(&bytes);
        assert!(key.decrypt("session", &tampered).is_none());
    }

    #[test]
    fn decrypt_rejects_short_data() {
        let key = Key::generate();
        assert!(key.decrypt("session", "dG9vc2hvcnQ=").is_none());
    }

    #[test]
    fn decrypt_rejects_invalid_base64() {
        let key = Key::generate();
        assert!(key.decrypt("session", "not valid base64!!!").is_none());
    }

    #[test]
    fn encrypt_empty_value() {
        let key = Key::generate();
        let encrypted = key.encrypt("cookie", "");
        let decrypted = key.decrypt("cookie", &encrypted);
        assert_eq!(decrypted.as_deref(), Some(""));
    }

    // -----------------------------------------------------------------------
    // SignedCookieJar extractor tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn signed_jar_reads_verified_cookies() {
        let key = Key::generate();
        let signed_value = key.sign("user123");

        let client = TestClient::new(
            Router::new()
                .state(key)
                .get("/", |jar: SignedCookieJar| async move {
                    jar.get("session").unwrap_or("none").to_string()
                }),
        );

        let resp = client
            .get("/")
            .header("cookie", &format!("session={signed_value}"))
            .send()
            .await;

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.text().await, "user123");
    }

    #[tokio::test]
    async fn signed_jar_ignores_unsigned_cookies() {
        let key = Key::generate();

        let client = TestClient::new(
            Router::new()
                .state(key)
                .get("/", |jar: SignedCookieJar| async move {
                    format!("{}", jar.len())
                }),
        );

        let resp = client
            .get("/")
            .header("cookie", "plain=value; unsigned=data")
            .send()
            .await;

        assert_eq!(resp.text().await, "0");
    }

    #[tokio::test]
    async fn signed_jar_ignores_tampered_cookies() {
        let key = Key::generate();
        let mut signed = key.sign("real");
        signed.push_str("TAMPERED");

        let client = TestClient::new(
            Router::new()
                .state(key)
                .get("/", |jar: SignedCookieJar| async move {
                    format!("{}", jar.len())
                }),
        );

        let resp = client
            .get("/")
            .header("cookie", &format!("session={signed}"))
            .send()
            .await;

        assert_eq!(resp.text().await, "0");
    }

    #[tokio::test]
    async fn signed_jar_without_key_returns_500() {
        let client = TestClient::new(
            Router::new().get("/", |jar: SignedCookieJar| async move {
                jar.get("x").unwrap_or("none").to_string()
            }),
        );

        let resp = client.get("/").send().await;
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn signed_cookie_set_and_read_roundtrip() {
        use crate::extract::State;

        let key = Key::generate();

        let client = TestClient::new(
            Router::new()
                .state(key.clone())
                .get("/set", |key: State<Key>| async move {
                    key.0.signed_cookie("token", "abc123").path("/")
                })
                .get("/read", |jar: SignedCookieJar| async move {
                    jar.get("token").unwrap_or("none").to_string()
                }),
        );

        // Set the cookie
        let resp = client.get("/set").send().await;
        let set_cookie = resp.header("set-cookie").unwrap().to_string();

        // Extract the cookie value from Set-Cookie header
        let cookie_val = set_cookie
            .split(';')
            .next()
            .unwrap()
            .trim();

        // Read it back
        let resp = client
            .get("/read")
            .header("cookie", cookie_val)
            .send()
            .await;

        assert_eq!(resp.text().await, "abc123");
    }

    // -----------------------------------------------------------------------
    // PrivateCookieJar extractor tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn private_jar_reads_decrypted_cookies() {
        let key = Key::generate();
        let encrypted_value = key.encrypt("secret", "top-secret-data");

        let client = TestClient::new(
            Router::new()
                .state(key)
                .get("/", |jar: PrivateCookieJar| async move {
                    jar.get("secret").unwrap_or("none").to_string()
                }),
        );

        let resp = client
            .get("/")
            .header("cookie", &format!("secret={encrypted_value}"))
            .send()
            .await;

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.text().await, "top-secret-data");
    }

    #[tokio::test]
    async fn private_jar_ignores_unencrypted_cookies() {
        let key = Key::generate();

        let client = TestClient::new(
            Router::new()
                .state(key)
                .get("/", |jar: PrivateCookieJar| async move {
                    format!("{}", jar.len())
                }),
        );

        let resp = client
            .get("/")
            .header("cookie", "plain=value; unsigned=data")
            .send()
            .await;

        assert_eq!(resp.text().await, "0");
    }

    #[tokio::test]
    async fn private_jar_rejects_wrong_name() {
        let key = Key::generate();
        let encrypted = key.encrypt("original", "data");

        let client = TestClient::new(
            Router::new()
                .state(key)
                .get("/", |jar: PrivateCookieJar| async move {
                    jar.get("swapped").unwrap_or("none").to_string()
                }),
        );

        // Try to use ciphertext from "original" cookie under name "swapped"
        let resp = client
            .get("/")
            .header("cookie", &format!("swapped={encrypted}"))
            .send()
            .await;

        assert_eq!(resp.text().await, "none");
    }

    #[tokio::test]
    async fn private_jar_without_key_returns_500() {
        let client = TestClient::new(
            Router::new().get("/", |jar: PrivateCookieJar| async move {
                jar.get("x").unwrap_or("none").to_string()
            }),
        );

        let resp = client.get("/").send().await;
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn private_cookie_set_and_read_roundtrip() {
        use crate::extract::State;

        let key = Key::generate();

        let client = TestClient::new(
            Router::new()
                .state(key.clone())
                .get("/set", |key: State<Key>| async move {
                    key.0.private_cookie("secret", "classified").path("/")
                })
                .get("/read", |jar: PrivateCookieJar| async move {
                    jar.get("secret").unwrap_or("none").to_string()
                }),
        );

        // Set the encrypted cookie
        let resp = client.get("/set").send().await;
        let set_cookie = resp.header("set-cookie").unwrap().to_string();

        // Extract cookie value from header
        let cookie_val = set_cookie.split(';').next().unwrap().trim();

        // Read it back
        let resp = client
            .get("/read")
            .header("cookie", cookie_val)
            .send()
            .await;

        assert_eq!(resp.text().await, "classified");
    }

    #[tokio::test]
    async fn signed_and_private_jars_independent() {
        let key = Key::generate();
        let signed_value = key.sign("signed-data");
        let encrypted_value = key.encrypt("encrypted", "encrypted-data");

        let client = TestClient::new(
            Router::new()
                .state(key)
                .get(
                    "/",
                    |signed: SignedCookieJar, private: PrivateCookieJar| async move {
                        Json(serde_json::json!({
                            "signed": signed.get("signed"),
                            "encrypted": private.get("encrypted"),
                        }))
                    },
                ),
        );

        let resp = client
            .get("/")
            .header(
                "cookie",
                &format!("signed={signed_value}; encrypted={encrypted_value}"),
            )
            .send()
            .await;

        let body: serde_json::Value = resp.json().await;
        assert_eq!(body["signed"], "signed-data");
        assert_eq!(body["encrypted"], "encrypted-data");
    }
}
