//! WebAuthn registration ceremony.

use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::cbor::{parse_cose_key, CoseKey};
use crate::config::WebAuthnConfig;
use crate::credential::PublicKeyCredential;
use crate::error::WebAuthnError;

/// Options sent to the browser to initiate a registration.
#[derive(Debug, Clone, Serialize)]
pub struct RegistrationOptions {
    pub rp_id:      String,
    pub rp_name:    String,
    pub user_id:    String,
    pub username:   String,
    pub challenge:  String, // base64url
    pub timeout_ms: u64,
}

/// Server-side state saved while waiting for the browser response.
#[derive(Debug, Clone)]
pub struct RegistrationChallenge {
    /// Raw challenge bytes (must match what was sent to the browser).
    pub challenge_bytes: Vec<u8>,
    pub user_id: String,
}

/// The browser's registration response (attestation object + client data JSON).
#[derive(Debug, Clone, Deserialize)]
pub struct RegistrationResponse {
    /// Base64URL-encoded credential ID assigned by the authenticator.
    pub id: String,
    /// Base64URL-encoded clientDataJSON bytes.
    pub client_data_json: String,
    /// Base64URL-encoded CBOR attestation object.
    pub attestation_object: String,
}

/// Generate registration options and a server-side challenge to store.
pub fn begin_registration(
    config: &WebAuthnConfig,
    user_id: impl Into<String>,
    username: impl Into<String>,
) -> (RegistrationOptions, RegistrationChallenge) {
    let challenge_bytes = random_bytes(32);
    let challenge_b64 = b64url_encode(&challenge_bytes);
    let user_id = user_id.into();

    let options = RegistrationOptions {
        rp_id:      config.rp_id.clone(),
        rp_name:    config.rp_name.clone(),
        user_id:    user_id.clone(),
        username:   username.into(),
        challenge:  challenge_b64,
        timeout_ms: 60_000,
    };
    let state = RegistrationChallenge { challenge_bytes, user_id };
    (options, state)
}

/// Verify the registration response and return a credential to store.
pub fn finish_registration(
    config: &WebAuthnConfig,
    challenge: &RegistrationChallenge,
    response: &RegistrationResponse,
) -> Result<PublicKeyCredential, WebAuthnError> {
    // 1. Decode and parse clientDataJSON
    let client_data_bytes = b64url_decode(&response.client_data_json)?;
    let client_data: Value = serde_json::from_slice(&client_data_bytes)
        .map_err(|e| WebAuthnError::Json(e.to_string()))?;

    // 2. Verify type
    if client_data.get("type").and_then(|v| v.as_str()) != Some("webauthn.create") {
        return Err(WebAuthnError::UnsupportedCredentialType);
    }

    // 3. Verify challenge
    let got_challenge = client_data
        .get("challenge")
        .and_then(|v| v.as_str())
        .ok_or_else(|| WebAuthnError::MissingField("challenge".into()))?;
    let expected_challenge = b64url_encode(&challenge.challenge_bytes);
    if got_challenge != expected_challenge {
        return Err(WebAuthnError::ChallengeMismatch);
    }

    // 4. Verify origin
    let got_origin = client_data
        .get("origin")
        .and_then(|v| v.as_str())
        .ok_or_else(|| WebAuthnError::MissingField("origin".into()))?;
    if got_origin != config.origin {
        return Err(WebAuthnError::OriginMismatch);
    }

    // 5. Decode and parse attestation object (CBOR)
    let att_bytes = b64url_decode(&response.attestation_object)?;
    let (auth_data, _fmt) = parse_attestation_object(&att_bytes)?;

    // 6. Verify RP ID hash (first 32 bytes of authData)
    if auth_data.len() < 37 {
        return Err(WebAuthnError::Cbor("authData too short".into()));
    }
    let rp_id_hash = &auth_data[..32];
    let expected_hash: Vec<u8> = Sha256::digest(config.rp_id.as_bytes()).to_vec();
    if rp_id_hash != expected_hash.as_slice() {
        return Err(WebAuthnError::OriginMismatch);
    }

    // 7. Check flags byte (byte 32)
    let flags = auth_data[32];
    let up_flag = (flags & 0x01) != 0; // User Present
    let uv_flag = (flags & 0x04) != 0; // User Verified
    if !up_flag {
        return Err(WebAuthnError::UserNotVerified);
    }
    if config.require_user_verification && !uv_flag {
        return Err(WebAuthnError::UserNotVerified);
    }

    // 8. Parse sign count (bytes 33–36, big-endian u32)
    let sign_count = u32::from_be_bytes([auth_data[33], auth_data[34], auth_data[35], auth_data[36]]);

    // 9. Extract COSE public key from attested credential data (byte 55 onwards, simplified)
    let cose_key_bytes = extract_cose_key(&auth_data)?;
    let cose_key: CoseKey = parse_cose_key(&cose_key_bytes)?;
    if cose_key.alg != -7 {
        return Err(WebAuthnError::UnsupportedAlgorithm);
    }

    Ok(PublicKeyCredential {
        id: response.id.clone(),
        public_key_cbor: cose_key_bytes,
        sign_count,
        is_platform: true,
    })
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

pub(crate) fn b64url_encode(data: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(data)
}

pub(crate) fn b64url_decode(s: &str) -> Result<Vec<u8>, WebAuthnError> {
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(s)
        .map_err(|e| WebAuthnError::Base64(e.to_string()))
}

fn random_bytes(n: usize) -> Vec<u8> {
    use rand::RngCore;
    let mut bytes = vec![0u8; n];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes
}

/// Parse CBOR attestation object — we only need authData and fmt for "none" attestation.
fn parse_attestation_object(data: &[u8]) -> Result<(Vec<u8>, String), WebAuthnError> {
    // Simple CBOR map decoder: find "authData" and "fmt" keys
    let mut pos = 0usize;
    let b = *data.get(pos).ok_or_else(|| WebAuthnError::Cbor("empty".into()))?;
    if b >> 5 != 5 {
        return Err(WebAuthnError::Cbor("attestation object must be a CBOR map".into()));
    }
    let map_len = (b & 0x1f) as usize;
    pos += 1;

    let mut auth_data: Option<Vec<u8>> = None;
    let mut fmt = String::new();

    for _ in 0..map_len {
        // Read key (text string)
        let key = read_cbor_text(data, &mut pos)?;
        match key.as_str() {
            "authData" => { auth_data = Some(read_cbor_bytes(data, &mut pos)?); }
            "fmt"      => { fmt = read_cbor_text(data, &mut pos)?; }
            _          => { skip_cbor_value(data, &mut pos)?; }
        }
    }

    let auth_data = auth_data.ok_or_else(|| WebAuthnError::MissingField("authData".into()))?;
    Ok((auth_data, fmt))
}

/// Extract the COSE key bytes from attested credential data.
/// authData layout: rpIdHash(32) | flags(1) | signCount(4) | aaguid(16) | credIdLen(2) | credId(n) | coseKey(...)
fn extract_cose_key(auth_data: &[u8]) -> Result<Vec<u8>, WebAuthnError> {
    if auth_data.len() < 55 {
        return Err(WebAuthnError::Cbor("authData too short for attested credential data".into()));
    }
    // 32 (rp hash) + 1 (flags) + 4 (sign count) + 16 (aaguid) = 53
    let cred_id_len = u16::from_be_bytes([auth_data[53], auth_data[54]]) as usize;
    let cose_start = 55 + cred_id_len;
    if cose_start > auth_data.len() {
        return Err(WebAuthnError::Cbor("authData too short for COSE key".into()));
    }
    Ok(auth_data[cose_start..].to_vec())
}

fn read_cbor_text(data: &[u8], pos: &mut usize) -> Result<String, WebAuthnError> {
    let b = *data.get(*pos).ok_or_else(|| WebAuthnError::Cbor("truncated".into()))?;
    if b >> 5 != 3 {
        return Err(WebAuthnError::Cbor(format!("expected text, got major {}", b >> 5)));
    }
    let len = (b & 0x1f) as usize;
    *pos += 1;
    if *pos + len > data.len() {
        return Err(WebAuthnError::Cbor("text string overruns buffer".into()));
    }
    let s = std::str::from_utf8(&data[*pos..*pos + len])
        .map_err(|_| WebAuthnError::Cbor("invalid UTF-8 in text".into()))?
        .to_string();
    *pos += len;
    Ok(s)
}

fn read_cbor_bytes(data: &[u8], pos: &mut usize) -> Result<Vec<u8>, WebAuthnError> {
    let b = *data.get(*pos).ok_or_else(|| WebAuthnError::Cbor("truncated".into()))?;
    if b >> 5 != 2 {
        return Err(WebAuthnError::Cbor(format!("expected bytes, got major {}", b >> 5)));
    }
    *pos += 1;
    // Handle 1-byte length prefix (0x58 = major 2, additional 24 = 1-byte length follows)
    let len = if (b & 0x1f) == 24 {
        let l = *data.get(*pos).ok_or_else(|| WebAuthnError::Cbor("truncated".into()))? as usize;
        *pos += 1;
        l
    } else if (b & 0x1f) == 25 {
        let hi = *data.get(*pos).ok_or_else(|| WebAuthnError::Cbor("truncated".into()))? as usize;
        let lo = *data.get(*pos + 1).ok_or_else(|| WebAuthnError::Cbor("truncated".into()))? as usize;
        *pos += 2;
        (hi << 8) | lo
    } else {
        (b & 0x1f) as usize
    };
    if *pos + len > data.len() {
        return Err(WebAuthnError::Cbor("bytes overrun buffer".into()));
    }
    let out = data[*pos..*pos + len].to_vec();
    *pos += len;
    Ok(out)
}

fn skip_cbor_value(data: &[u8], pos: &mut usize) -> Result<(), WebAuthnError> {
    let b = *data.get(*pos).ok_or_else(|| WebAuthnError::Cbor("truncated".into()))?;
    let major = b >> 5;
    match major {
        0 | 1 => { *pos += 1; } // simple small int
        2 | 3 => {
            let len = (b & 0x1f) as usize;
            *pos += 1 + len;
        }
        _ => return Err(WebAuthnError::Cbor(format!("skip: unsupported major {major}"))),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn begin_registration_produces_32_byte_challenge() {
        let config = WebAuthnConfig::new("example.com", "https://example.com");
        let (_opts, state) = begin_registration(&config, "u1", "alice");
        assert_eq!(state.challenge_bytes.len(), 32);
    }

    #[test]
    fn begin_registration_options_fields() {
        let config = WebAuthnConfig::new("example.com", "https://example.com")
            .rp_name("Example Corp");
        let (opts, _) = begin_registration(&config, "u1", "alice");
        assert_eq!(opts.rp_id, "example.com");
        assert_eq!(opts.rp_name, "Example Corp");
        assert_eq!(opts.username, "alice");
        assert_eq!(opts.timeout_ms, 60_000);
    }

    #[test]
    fn begin_registration_challenge_is_base64url() {
        let config = WebAuthnConfig::new("example.com", "https://example.com");
        let (opts, state) = begin_registration(&config, "u1", "alice");
        let decoded = b64url_decode(&opts.challenge).unwrap();
        assert_eq!(decoded, state.challenge_bytes);
    }

    #[test]
    fn begin_registration_unique_challenges() {
        let config = WebAuthnConfig::new("example.com", "https://example.com");
        let (_, s1) = begin_registration(&config, "u1", "alice");
        let (_, s2) = begin_registration(&config, "u2", "bob");
        assert_ne!(s1.challenge_bytes, s2.challenge_bytes);
    }

    #[test]
    fn b64url_roundtrip() {
        let data = b"hello world 123";
        let encoded = b64url_encode(data);
        let decoded = b64url_decode(&encoded).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn b64url_decode_invalid_fails() {
        assert!(b64url_decode("!!!").is_err());
    }
}
