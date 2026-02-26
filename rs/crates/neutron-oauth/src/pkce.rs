//! PKCE (Proof Key for Code Exchange) — RFC 7636.
//!
//! Generates a random `code_verifier` and derives a `code_challenge` using
//! the S256 method (BASE64URL-NoPad(SHA256(verifier))).

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use rand::RngCore;
use sha2::{Digest, Sha256};

// ---------------------------------------------------------------------------
// PkceChallenge
// ---------------------------------------------------------------------------

/// A PKCE challenge pair — keep the `verifier` secret (in the state cookie)
/// and send the `challenge` to the provider in the authorization URL.
#[derive(Debug, Clone)]
pub struct PkceChallenge {
    /// The secret value to send with the token exchange request.
    pub verifier: String,
    /// `BASE64URL-NoPad(SHA256(verifier))` — sent in the authorization URL.
    pub challenge: String,
}

impl PkceChallenge {
    /// Generate a fresh PKCE challenge using a cryptographically random
    /// 32-byte verifier (produces a 43-character base64url string).
    pub fn new() -> Self {
        let mut bytes = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut bytes);
        let verifier  = URL_SAFE_NO_PAD.encode(bytes);
        let challenge = Self::derive_challenge(&verifier);
        Self { verifier, challenge }
    }

    /// Derive a challenge from an existing verifier string.
    pub fn from_verifier(verifier: impl Into<String>) -> Self {
        let verifier  = verifier.into();
        let challenge = Self::derive_challenge(&verifier);
        Self { verifier, challenge }
    }

    fn derive_challenge(verifier: &str) -> String {
        let hash = Sha256::digest(verifier.as_bytes());
        URL_SAFE_NO_PAD.encode(hash)
    }
}

impl Default for PkceChallenge {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verifier_is_base64url_43_chars() {
        let pkce = PkceChallenge::new();
        // 32 bytes → 43 base64url chars (no padding)
        assert_eq!(pkce.verifier.len(), 43);
        // Only base64url characters
        assert!(pkce.verifier.chars().all(|c| c.is_alphanumeric() || c == '-' || c == '_'));
    }

    #[test]
    fn challenge_is_base64url_43_chars() {
        let pkce = PkceChallenge::new();
        // SHA256(32 bytes) = 32 bytes → 43 base64url chars
        assert_eq!(pkce.challenge.len(), 43);
    }

    #[test]
    fn challenge_is_sha256_of_verifier() {
        // Known test vector from RFC 7636 §B
        // verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
        // challenge = "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        let pkce = PkceChallenge::from_verifier("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk");
        assert_eq!(pkce.challenge, "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM");
    }

    #[test]
    fn two_challenges_are_different() {
        let a = PkceChallenge::new();
        let b = PkceChallenge::new();
        assert_ne!(a.verifier, b.verifier);
        assert_ne!(a.challenge, b.challenge);
    }

    #[test]
    fn from_verifier_round_trips() {
        let original = PkceChallenge::new();
        let derived  = PkceChallenge::from_verifier(&original.verifier);
        assert_eq!(derived.challenge, original.challenge);
    }
}
