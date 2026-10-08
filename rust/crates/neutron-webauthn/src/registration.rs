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
    pub rp_id: String,
    pub rp_name: String,
    pub user_id: String,
    pub username: String,
    pub challenge: String, // base64url
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
        rp_id: config.rp_id.clone(),
        rp_name: config.rp_name.clone(),
        user_id: user_id.clone(),
        username: username.into(),
        challenge: challenge_b64,
        timeout_ms: 60_000,
    };
    let state = RegistrationChallenge {
        challenge_bytes,
        user_id,
    };
    (options, state)
}

/// Verify the registration response and return a credential to store.
pub fn finish_registration(
    config: &WebAuthnConfig,
    challenge: &RegistrationChallenge,
    response: &RegistrationResponse,
) -> Result<PublicKeyCredential, WebAuthnError> {
    if response.attestation_object.len() > 90000
        || response.client_data_json.len() > 8192
        || response.id.len() > 2048
    {
        return Err(WebAuthnError::Cbor(
            "registration input exceeds supported limits".into(),
        ));
    }
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
    let (auth_data, fmt, att_stmt) = parse_attestation_object(&att_bytes)?;

    // 5a. Attestation format contract: this crate verifies only the `none`
    // format. A browser registration with attestation elided produces
    // fmt="none" and attStmt={} (an EMPTY CBOR map) — the previous parser
    // rejected that map outright, so every standard registration failed.
    // Anything else (unknown format, non-empty statement) is refused rather
    // than silently treated as verified.
    if fmt != "none" {
        return Err(WebAuthnError::UnsupportedAttestationFormat(fmt));
    }
    if att_stmt != [0xa0] {
        return Err(WebAuthnError::Cbor(
            "fmt \"none\" requires an empty attStmt map".into(),
        ));
    }

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
    let at_flag = (flags & 0x40) != 0; // Attested credential data included
    if !up_flag {
        return Err(WebAuthnError::UserNotVerified);
    }
    if config.require_user_verification && !uv_flag {
        return Err(WebAuthnError::UserNotVerified);
    }
    if !at_flag {
        return Err(WebAuthnError::Cbor(
            "authData AT flag not set: no attested credential data".into(),
        ));
    }

    // 8. Parse sign count (bytes 33–36, big-endian u32)
    let sign_count =
        u32::from_be_bytes([auth_data[33], auth_data[34], auth_data[35], auth_data[36]]);

    // 9. Extract COSE public key from attested credential data (byte 55 onwards, simplified)
    let cose_key_bytes = extract_cose_key(&auth_data)?;
    let cose_key: CoseKey = parse_cose_key(&cose_key_bytes)?;
    if cose_key.alg != -7 || cose_key.kty != 2 || cose_key.crv != 1 {
        return Err(WebAuthnError::UnsupportedAlgorithm);
    }

    if cose_key.x.len() != 32 || cose_key.y.len() != 32 {
        return Err(WebAuthnError::Cbor(
            "invalid P-256 coordinate length".into(),
        ));
    }
    let mut point = [0u8; 65];
    point[0] = 4;
    point[1..33].copy_from_slice(&cose_key.x);
    point[33..].copy_from_slice(&cose_key.y);
    p256::ecdsa::VerifyingKey::from_sec1_bytes(&point)
        .map_err(|_| WebAuthnError::Cbor("invalid P-256 public point".into()))?;

    // 9a. The credential ID embedded in attested credential data must match
    // the response's top-level `id` — a mismatch means the returned handle is
    // not the one the authenticator attested.
    if auth_data.len() >= 55 {
        let cred_id_len = u16::from_be_bytes([auth_data[53], auth_data[54]]) as usize;
        if 55 + cred_id_len <= auth_data.len() {
            let embedded_id = &auth_data[55..55 + cred_id_len];
            let response_id = b64url_decode(&response.id)?;
            if embedded_id != response_id.as_slice() {
                return Err(WebAuthnError::Cbor(
                    "credential ID in authData does not match response id".into(),
                ));
            }
        }
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

/// Parse CBOR attestation object. Returns `(authData, fmt, attStmt bytes)`.
///
/// The map keys are read in order; `attStmt` is captured raw so callers can
/// enforce the `none`-format contract (empty map) instead of guessing.
fn parse_attestation_object(data: &[u8]) -> Result<(Vec<u8>, String, Vec<u8>), WebAuthnError> {
    // Simple CBOR map decoder: find "authData", "fmt" and "attStmt" keys
    let mut pos = 0usize;
    let b = *data
        .get(pos)
        .ok_or_else(|| WebAuthnError::Cbor("empty".into()))?;
    if b >> 5 != 5 {
        return Err(WebAuthnError::Cbor(
            "attestation object must be a CBOR map".into(),
        ));
    }
    if data.len() > 65536 {
        return Err(WebAuthnError::Cbor("attestation too large".into()));
    }
    let map_len = read_cbor_head_len(data, &mut pos, b & 0x1f)?;
    if map_len > 32 {
        return Err(WebAuthnError::Cbor("too many attestation fields".into()));
    }

    let mut auth_data: Option<Vec<u8>> = None;
    let mut fmt = String::new();
    let mut att_stmt: Vec<u8> = Vec::new();

    let mut seen = std::collections::HashSet::new();
    for _ in 0..map_len {
        // Read key (text string)
        let key = read_cbor_text(data, &mut pos)?;
        if !seen.insert(key.clone()) {
            return Err(WebAuthnError::Cbor("duplicate attestation field".into()));
        }
        match key.as_str() {
            "authData" => {
                auth_data = Some(read_cbor_bytes(data, &mut pos)?);
            }
            "fmt" => {
                fmt = read_cbor_text(data, &mut pos)?;
            }
            "attStmt" => {
                let start = pos;
                skip_cbor_value(data, &mut pos)?;
                att_stmt = data[start..pos].to_vec();
            }
            _ => {
                skip_cbor_value(data, &mut pos)?;
            }
        }
    }

    if pos != data.len() {
        return Err(WebAuthnError::Cbor("trailing attestation data".into()));
    }
    let auth_data = auth_data.ok_or_else(|| WebAuthnError::MissingField("authData".into()))?;
    Ok((auth_data, fmt, att_stmt))
}

/// Extract the COSE key bytes from attested credential data.
/// authData layout: rpIdHash(32) | flags(1) | signCount(4) | aaguid(16) | credIdLen(2) | credId(n) | coseKey(...)
fn extract_cose_key(auth_data: &[u8]) -> Result<Vec<u8>, WebAuthnError> {
    if auth_data.len() < 55 {
        return Err(WebAuthnError::Cbor(
            "authData too short for attested credential data".into(),
        ));
    }
    // 32 (rp hash) + 1 (flags) + 4 (sign count) + 16 (aaguid) = 53
    let cred_id_len = u16::from_be_bytes([auth_data[53], auth_data[54]]) as usize;
    let cose_start = 55 + cred_id_len;
    if cose_start > auth_data.len() {
        return Err(WebAuthnError::Cbor(
            "authData too short for COSE key".into(),
        ));
    }
    Ok(auth_data[cose_start..].to_vec())
}

fn read_cbor_text(data: &[u8], pos: &mut usize) -> Result<String, WebAuthnError> {
    let b = *data
        .get(*pos)
        .ok_or_else(|| WebAuthnError::Cbor("truncated".into()))?;
    if b >> 5 != 3 {
        return Err(WebAuthnError::Cbor(format!(
            "expected text, got major {}",
            b >> 5
        )));
    }
    let len = read_cbor_head_len(data, pos, b & 0x1f)?;
    if len > data.len().saturating_sub(*pos) {
        return Err(WebAuthnError::Cbor("text string overruns buffer".into()));
    }
    let s = std::str::from_utf8(&data[*pos..*pos + len])
        .map_err(|_| WebAuthnError::Cbor("invalid UTF-8 in text".into()))?
        .to_string();
    *pos += len;
    Ok(s)
}

fn read_cbor_bytes(data: &[u8], pos: &mut usize) -> Result<Vec<u8>, WebAuthnError> {
    let b = *data
        .get(*pos)
        .ok_or_else(|| WebAuthnError::Cbor("truncated".into()))?;
    if b >> 5 != 2 {
        return Err(WebAuthnError::Cbor(format!(
            "expected bytes, got major {}",
            b >> 5
        )));
    }
    let len = read_cbor_head_len(data, pos, b & 0x1f)?;
    if len > data.len().saturating_sub(*pos) {
        return Err(WebAuthnError::Cbor("bytes overrun buffer".into()));
    }
    let out = data[*pos..*pos + len].to_vec();
    *pos += len;
    Ok(out)
}

fn skip_cbor_value(data: &[u8], pos: &mut usize) -> Result<(), WebAuthnError> {
    fn skip_recursive(data: &[u8], pos: &mut usize, depth: usize) -> Result<(), WebAuthnError> {
        if depth > 8 {
            return Err(WebAuthnError::Cbor("skip: nesting too deep".into()));
        }
        let b = *data
            .get(*pos)
            .ok_or_else(|| WebAuthnError::Cbor("truncated".into()))?;
        let major = b >> 5;
        let info = b & 0x1f;
        match major {
            0 | 1 => {
                // Unsigned/negative integer: value in the info byte or the
                // following 1/2/4/8 bytes.
                let extra = match info {
                    0..=23 => 0,
                    24 => 1,
                    25 => 2,
                    26 => 4,
                    27 => 8,
                    _ => return Err(WebAuthnError::Cbor("skip: reserved integer length".into())),
                };
                *pos += 1 + extra;
            }
            2 | 3 => {
                // Byte/text string with explicit length.
                let len = read_cbor_head_len(data, pos, info)?;
                if len > data.len().saturating_sub(*pos) {
                    return Err(WebAuthnError::Cbor("value overruns buffer".into()));
                }
                *pos += len;
            }
            4 | 5 => {
                // Array/map: skip each entry (key then value for maps).
                let len = read_cbor_head_len(data, pos, info)?;
                let entries = if major == 5 {
                    len.checked_mul(2)
                } else {
                    Some(len)
                }
                .ok_or_else(|| WebAuthnError::Cbor("container length overflow".into()))?;
                if entries > data.len().saturating_sub(*pos) {
                    return Err(WebAuthnError::Cbor("container overruns buffer".into()));
                }
                for _ in 0..entries {
                    skip_recursive(data, pos, depth + 1)?;
                }
            }
            7 => {
                // Simple value / float.
                let extra = match info {
                    0..=23 => 0,
                    24 => 1,
                    25 => 2,
                    26 => 4,
                    27 => 8,
                    _ => return Err(WebAuthnError::Cbor("skip: reserved simple length".into())),
                };
                *pos += 1 + extra;
            }
            _ => return Err(WebAuthnError::Cbor("CBOR tags unsupported".into())),
        }
        if *pos > data.len() {
            return Err(WebAuthnError::Cbor("value overruns buffer".into()));
        }
        Ok(())
    }
    skip_recursive(data, pos, 0)
}

/// Read a CBOR head length: info 0..=23 is the length itself; 24/25/26/27
/// carry it in the following 1/2/4/8 bytes. Indefinite lengths (31) are
/// refused — attestation objects from browsers always use definite lengths.
fn read_cbor_head_len(data: &[u8], pos: &mut usize, info: u8) -> Result<usize, WebAuthnError> {
    *pos += 1;
    match info {
        0..=23 => Ok(info as usize),
        24 => {
            let l = *data
                .get(*pos)
                .ok_or_else(|| WebAuthnError::Cbor("truncated".into()))?
                as usize;
            *pos += 1;
            Ok(l)
        }
        25 => {
            let hi = *data
                .get(*pos)
                .ok_or_else(|| WebAuthnError::Cbor("truncated".into()))?
                as usize;
            let lo = *data
                .get(*pos + 1)
                .ok_or_else(|| WebAuthnError::Cbor("truncated".into()))?
                as usize;
            *pos += 2;
            Ok((hi << 8) | lo)
        }
        26 => {
            let mut v = 0usize;
            for i in 0..4 {
                let b = *data
                    .get(*pos + i)
                    .ok_or_else(|| WebAuthnError::Cbor("truncated".into()))?
                    as usize;
                v = v
                    .checked_mul(256)
                    .and_then(|v| v.checked_add(b))
                    .ok_or_else(|| WebAuthnError::Cbor("CBOR length overflow".into()))?;
            }
            *pos += 4;
            Ok(v)
        }
        27 => {
            let mut v = 0usize;
            for i in 0..8 {
                let b = *data
                    .get(*pos + i)
                    .ok_or_else(|| WebAuthnError::Cbor("truncated".into()))?
                    as usize;
                v = v
                    .checked_mul(256)
                    .and_then(|v| v.checked_add(b))
                    .ok_or_else(|| WebAuthnError::Cbor("CBOR length overflow".into()))?;
            }
            *pos += 8;
            Ok(v)
        }
        _ => Err(WebAuthnError::Cbor(
            "skip: indefinite lengths are not accepted".into(),
        )),
    }
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
        let config =
            WebAuthnConfig::new("example.com", "https://example.com").rp_name("Example Corp");
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

    // ------------------------------------------------------------------
    // RS-17 regressions: complete browser-shaped `none`-attestation
    // registration fixture, plus the malformed/negative variants.
    // ------------------------------------------------------------------

    /// Build a valid CBOR attestation object exactly as a browser serializes
    /// it for `attestation: "none"`: {"fmt":"none","authData":…,"attStmt":{}}
    fn build_none_attestation_object(rp_id: &str, cred_id: &[u8], flags: u8) -> Vec<u8> {
        // COSE ES256 key: {1:2, 3:-7, -1:1, -2:x, -3:y}
        let mut cose = vec![0xa5, 0x01, 0x02, 0x03, 0x26, 0x20, 0x01, 0x21, 0x58, 0x20];
        // SEC 2 P-256 generator point, independent of this parser/encoder.
        let x = "6b17d1f2e12c4247f8bce6e563a440f277037d812deb33a0f4a13945d898c296";
        let y = "4fe342e2fe1a7f9b8ee7eb4a7c0f9e162bce33576b315ececbb6406837bf51f5";
        let bytes = |s: &str| {
            (0..s.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
                .collect::<Vec<_>>()
        };
        cose.extend_from_slice(&bytes(x));
        cose.extend_from_slice(&[0x22, 0x58, 0x20]);
        cose.extend_from_slice(&bytes(y));

        let mut auth_data = Sha256::digest(rp_id.as_bytes()).to_vec();
        auth_data.push(flags); // UP | UV | AT for the happy path
        auth_data.extend_from_slice(&1u32.to_be_bytes()); // sign count
        auth_data.extend_from_slice(&[0u8; 16]); // aaguid
        auth_data.extend_from_slice(&(cred_id.len() as u16).to_be_bytes());
        auth_data.extend_from_slice(cred_id);
        auth_data.extend_from_slice(&cose);

        let mut out = vec![0xa3]; // map(3)
        out.push(0x63);
        out.extend_from_slice(b"fmt"); // text(3) "fmt"
        out.push(0x64);
        out.extend_from_slice(b"none"); // text(4) "none"
        out.push(0x68);
        out.extend_from_slice(b"authData"); // text(8) "authData"
        out.push(0x58); // bytes(1-byte length): authData is always > 23 bytes
        out.push(auth_data.len() as u8);
        out.extend_from_slice(&auth_data);
        out.push(0x67);
        out.extend_from_slice(b"attStmt"); // text(7) "attStmt"
        out.push(0xa0); // map(0) — the empty attestation statement
        out
    }

    fn build_response(
        rp_id: &str,
        origin: &str,
        challenge: &str,
        cred_id: &[u8],
        flags: u8,
    ) -> RegistrationResponse {
        let client_data = serde_json::json!({
            "type": "webauthn.create",
            "challenge": challenge,
            "origin": origin,
        });
        RegistrationResponse {
            id: b64url_encode(cred_id),
            client_data_json: b64url_encode(client_data.to_string().as_bytes()),
            attestation_object: b64url_encode(&build_none_attestation_object(
                rp_id, cred_id, flags,
            )),
        }
    }

    /// The standard browser happy path: fmt=none + attStmt={} previously
    /// failed in `skip_cbor_value` (major type 5 rejected), so every ordinary
    /// registration was refused before a credential could be created.
    #[test]
    fn finish_registration_accepts_none_attestation_with_empty_map() {
        let config = WebAuthnConfig::new("example.com", "https://example.com");
        let (_opts, state) = begin_registration(&config, "u1", "alice");
        let cred_id = vec![7u8; 16];
        let response = build_response(
            "example.com",
            "https://example.com",
            &b64url_encode(&state.challenge_bytes),
            &cred_id,
            0x01 | 0x04 | 0x40,
        );
        let credential =
            finish_registration(&config, &state, &response).expect("none attestation must verify");
        assert_eq!(credential.sign_count, 1);
        assert!(!credential.public_key_cbor.is_empty());
    }

    #[test]
    fn finish_registration_rejects_missing_at_flag() {
        let config = WebAuthnConfig::new("example.com", "https://example.com");
        let (_opts, state) = begin_registration(&config, "u1", "alice");
        let cred_id = vec![7u8; 16];
        let response = build_response(
            "example.com",
            "https://example.com",
            &b64url_encode(&state.challenge_bytes),
            &cred_id,
            0x01 | 0x04, // UP | UV, no AT
        );
        let err = finish_registration(&config, &state, &response).unwrap_err();
        assert!(err.to_string().contains("AT flag"));
    }

    #[test]
    fn finish_registration_rejects_credential_id_mismatch() {
        let config = WebAuthnConfig::new("example.com", "https://example.com");
        let (_opts, state) = begin_registration(&config, "u1", "alice");
        let cred_id = vec![7u8; 16];
        let mut response = build_response(
            "example.com",
            "https://example.com",
            &b64url_encode(&state.challenge_bytes),
            &cred_id,
            0x01 | 0x04 | 0x40,
        );
        response.id = b64url_encode(&vec![9u8; 16]); // different handle
        let err = finish_registration(&config, &state, &response).unwrap_err();
        assert!(err.to_string().contains("does not match"));
    }

    /// An attestation format we do not verify must be refused, not accepted
    /// as if it were verified.
    #[test]
    fn finish_registration_rejects_non_none_format() {
        let config = WebAuthnConfig::new("example.com", "https://example.com");
        let (_opts, state) = begin_registration(&config, "u1", "alice");
        let cred_id = vec![7u8; 16];
        let mut response = build_response(
            "example.com",
            "https://example.com",
            &b64url_encode(&state.challenge_bytes),
            &cred_id,
            0x01 | 0x04 | 0x40,
        );
        let mut att = b64url_decode(&response.attestation_object).unwrap();
        // "none" -> "pack" (same 4-byte text length keeps CBOR valid)
        let idx = att
            .windows(4)
            .position(|w| w == b"none")
            .expect("fmt string present");
        att[idx..idx + 4].copy_from_slice(b"pack");
        response.attestation_object = b64url_encode(&att);
        let err = finish_registration(&config, &state, &response).unwrap_err();
        assert!(matches!(
            err,
            WebAuthnError::UnsupportedAttestationFormat(_)
        ));
    }

    /// fmt=none with a NON-empty attStmt is malformed per WebAuthn Level 2
    /// (the none format defines an empty statement) and must be rejected.
    #[test]
    fn finish_registration_rejects_non_empty_attstmt() {
        let config = WebAuthnConfig::new("example.com", "https://example.com");
        let (_opts, state) = begin_registration(&config, "u1", "alice");
        let cred_id = vec![7u8; 16];
        let mut response = build_response(
            "example.com",
            "https://example.com",
            &b64url_encode(&state.challenge_bytes),
            &cred_id,
            0x01 | 0x04 | 0x40,
        );
        let mut att = b64url_decode(&response.attestation_object).unwrap();
        let idx = att
            .windows(1)
            .rposition(|w| w == [0xa0])
            .expect("empty attStmt map present");
        // map(0) -> map(1) with a null entry
        att[idx] = 0xa1;
        att.push(0xf6); // null value (key would be needed, so this is malformed)
        response.attestation_object = b64url_encode(&att);
        assert!(finish_registration(&config, &state, &response).is_err());
    }

    #[test]
    fn tagged_attestation_unknown_field_is_error_without_panic() {
        let mut data = build_none_attestation_object("example.com", &[1; 16], 0x45);
        data[0] = 0xa4;
        data.extend_from_slice(&[0x61, b'x', 0xc0, 0]);
        assert!(parse_attestation_object(&data).is_err());
        let mut duplicate = build_none_attestation_object("example.com", &[1; 16], 0x45);
        duplicate[0] = 0xa4;
        duplicate.extend_from_slice(b"\x63fmt\x64none");
        assert!(parse_attestation_object(&duplicate).is_err());
        let mut trailing = build_none_attestation_object("example.com", &[1; 16], 0x45);
        trailing.push(0);
        assert!(parse_attestation_object(&trailing).is_err());
    }
    #[test]
    fn registration_refuses_off_curve_point_before_persistence() {
        let config = WebAuthnConfig::new("example.com", "https://example.com");
        let (_, state) = begin_registration(&config, "u1", "alice");
        let mut response = build_response(
            "example.com",
            "https://example.com",
            &b64url_encode(&state.challenge_bytes),
            &[7; 16],
            0x45,
        );
        let mut bytes = b64url_decode(&response.attestation_object).unwrap();
        // Final 32 bytes of y precede attStmt key and map (9 bytes).
        let y = bytes.len() - 9 - 32;
        bytes[y..y + 32].fill(0);
        response.attestation_object = b64url_encode(&bytes);
        assert!(finish_registration(&config, &state, &response).is_err());
    }
}
