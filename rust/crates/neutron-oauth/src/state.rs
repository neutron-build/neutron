//! Anti-CSRF state cookie generation and verification.
//!
//! The cookie value is `state|verifier|hmac` where the HMAC covers
//! `state|verifier` with the configured secret key.  Any tampering
//! invalidates the signature and causes the flow to abort.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use hmac::{Hmac, Mac};
use rand::RngCore;
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Cookie name used to store the OAuth flow state.
pub const OAUTH_STATE_COOKIE: &str = "__oauth_state";

// ---------------------------------------------------------------------------
// State generation
// ---------------------------------------------------------------------------

/// Generate a random URL-safe state string (32 bytes → 43 chars).
pub fn generate_state() -> String {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

/// Encode `state` and `verifier` into a signed cookie value.
///
/// Format: `{state}|{verifier}|{base64url(hmac)}`
pub fn encode_state_cookie(state: &str, verifier: &str, secret: &[u8]) -> String {
    let payload = format!("{state}|{verifier}");
    let sig     = hmac_sign(&payload, secret);
    format!("{payload}|{sig}")
}

/// Decode and verify a state cookie.
///
/// Returns `(state, verifier)` on success, or `None` if the cookie is
/// malformed or the HMAC is invalid.
pub fn decode_state_cookie(cookie: &str, secret: &[u8]) -> Option<(String, String)> {
    // Format: state | verifier | sig  (at least 3 pipe-separated parts)
    let parts: Vec<&str> = cookie.splitn(3, '|').collect();
    if parts.len() != 3 {
        return None;
    }
    let (state, verifier, sig_b64) = (parts[0], parts[1], parts[2]);

    let payload = format!("{state}|{verifier}");
    let expected = hmac_sign(&payload, secret);

    // Constant-time comparison
    if !constant_time_eq(sig_b64.as_bytes(), expected.as_bytes()) {
        return None;
    }

    Some((state.to_string(), verifier.to_string()))
}

fn hmac_sign(payload: &str, secret: &[u8]) -> String {
    let mut mac = HmacSha256::new_from_slice(secret)
        .expect("HMAC accepts any key size");
    mac.update(payload.as_bytes());
    URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b.iter()).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &[u8] = b"test-secret-key-for-unit-tests!!";

    #[test]
    fn generate_state_is_43_chars() {
        let s = generate_state();
        assert_eq!(s.len(), 43);
    }

    #[test]
    fn generate_state_is_unique() {
        let a = generate_state();
        let b = generate_state();
        assert_ne!(a, b);
    }

    #[test]
    fn encode_decode_round_trip() {
        let state    = "my-state-value";
        let verifier = "my-code-verifier";
        let cookie   = encode_state_cookie(state, verifier, SECRET);
        let result   = decode_state_cookie(&cookie, SECRET).unwrap();
        assert_eq!(result.0, state);
        assert_eq!(result.1, verifier);
    }

    #[test]
    fn tampered_state_fails_decode() {
        let cookie  = encode_state_cookie("state", "verifier", SECRET);
        let tampered = cookie.replace("state", "evil");
        assert!(decode_state_cookie(&tampered, SECRET).is_none());
    }

    #[test]
    fn wrong_secret_fails_decode() {
        let cookie = encode_state_cookie("state", "verifier", SECRET);
        assert!(decode_state_cookie(&cookie, b"wrong-secret").is_none());
    }

    #[test]
    fn malformed_cookie_returns_none() {
        assert!(decode_state_cookie("nodividers", SECRET).is_none());
        assert!(decode_state_cookie("only|one", SECRET).is_none());
        assert!(decode_state_cookie("", SECRET).is_none());
    }

    #[test]
    fn constant_time_eq_matches() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
    }
}
