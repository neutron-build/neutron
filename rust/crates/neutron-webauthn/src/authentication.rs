//! WebAuthn authentication ceremony.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use p256::ecdsa::{signature::Verifier, Signature, VerifyingKey};
use p256::EncodedPoint;

use crate::cbor::parse_cose_key;
use crate::config::WebAuthnConfig;
use crate::credential::StoredCredential;
use crate::error::WebAuthnError;
use crate::registration::{b64url_decode, b64url_encode};

/// Options sent to the browser to initiate an authentication assertion.
#[derive(Debug, Clone, Serialize)]
pub struct AuthenticationOptions {
    pub rp_id:         String,
    pub challenge:     String, // base64url
    pub timeout_ms:    u64,
    pub credential_id: String, // base64url hint to the browser
}

/// Server-side state saved while waiting for the browser assertion response.
#[derive(Debug, Clone)]
pub struct AuthenticationChallenge {
    pub challenge_bytes: Vec<u8>,
}

/// The browser's authentication response (assertion).
#[derive(Debug, Clone, Deserialize)]
pub struct AuthenticationResponse {
    /// Base64URL-encoded credential ID (identifies which key was used).
    pub id: String,
    /// Base64URL-encoded clientDataJSON.
    pub client_data_json: String,
    /// Base64URL-encoded authenticatorData.
    pub authenticator_data: String,
    /// Base64URL-encoded DER-encoded ECDSA signature.
    pub signature: String,
}

/// Generate authentication options and a challenge to store server-side.
pub fn begin_authentication(
    config: &WebAuthnConfig,
    credential_id: &str,
) -> (AuthenticationOptions, AuthenticationChallenge) {
    let challenge_bytes = random_bytes(32);
    let challenge_b64 = b64url_encode(&challenge_bytes);

    let options = AuthenticationOptions {
        rp_id: config.rp_id.clone(),
        challenge: challenge_b64,
        timeout_ms: 60_000,
        credential_id: credential_id.to_string(),
    };
    let state = AuthenticationChallenge { challenge_bytes };
    (options, state)
}

/// Verify an authentication assertion against a stored credential.
///
/// On success, returns the new signature counter (caller should update stored credential).
pub fn finish_authentication(
    config: &WebAuthnConfig,
    challenge: &AuthenticationChallenge,
    response: &AuthenticationResponse,
    stored: &StoredCredential,
) -> Result<u32, WebAuthnError> {
    // 1. Decode clientDataJSON
    let client_data_bytes = b64url_decode(&response.client_data_json)?;
    let client_data: Value = serde_json::from_slice(&client_data_bytes)
        .map_err(|e| WebAuthnError::Json(e.to_string()))?;

    // 2. Verify type
    if client_data.get("type").and_then(|v| v.as_str()) != Some("webauthn.get") {
        return Err(WebAuthnError::UnsupportedCredentialType);
    }

    // 3. Verify challenge
    let got_challenge = client_data
        .get("challenge")
        .and_then(|v| v.as_str())
        .ok_or_else(|| WebAuthnError::MissingField("challenge".into()))?;
    if got_challenge != b64url_encode(&challenge.challenge_bytes) {
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

    // 5. Decode authenticatorData
    let auth_data = b64url_decode(&response.authenticator_data)?;
    if auth_data.len() < 37 {
        return Err(WebAuthnError::Cbor("authData too short".into()));
    }

    // 6. Verify RP ID hash
    let rp_id_hash = &auth_data[..32];
    let expected_hash: Vec<u8> = Sha256::digest(config.rp_id.as_bytes()).to_vec();
    if rp_id_hash != expected_hash.as_slice() {
        return Err(WebAuthnError::OriginMismatch);
    }

    // 7. Check flags
    let flags = auth_data[32];
    let up_flag = (flags & 0x01) != 0;
    let uv_flag = (flags & 0x04) != 0;
    if !up_flag {
        return Err(WebAuthnError::UserNotVerified);
    }
    if config.require_user_verification && !uv_flag {
        return Err(WebAuthnError::UserNotVerified);
    }

    // 8. Extract sign count
    let sign_count = u32::from_be_bytes([auth_data[33], auth_data[34], auth_data[35], auth_data[36]]);

    // 9. Verify signature
    //    verificationData = authData || SHA-256(clientDataJSON)
    let client_data_hash: Vec<u8> = Sha256::digest(&client_data_bytes).to_vec();
    let mut verification_data = auth_data.to_vec();
    verification_data.extend_from_slice(&client_data_hash);

    verify_es256_signature(
        &stored.public_key_cbor,
        &verification_data,
        &b64url_decode(&response.signature)?,
    )?;

    Ok(sign_count)
}

/// Verify an ES256 (P-256 ECDSA) signature.
fn verify_es256_signature(
    public_key_cbor: &[u8],
    data: &[u8],
    sig_der: &[u8],
) -> Result<(), WebAuthnError> {
    let cose_key = parse_cose_key(public_key_cbor)?;
    if cose_key.alg != -7 {
        return Err(WebAuthnError::UnsupportedAlgorithm);
    }
    if cose_key.x.len() != 32 || cose_key.y.len() != 32 {
        return Err(WebAuthnError::InvalidSignature);
    }

    // Build uncompressed point: 0x04 || x || y (65 bytes)
    let mut uncompressed = [0u8; 65];
    uncompressed[0] = 0x04;
    uncompressed[1..33].copy_from_slice(&cose_key.x);
    uncompressed[33..].copy_from_slice(&cose_key.y);
    let point = EncodedPoint::from_bytes(uncompressed)
        .map_err(|_| WebAuthnError::InvalidSignature)?;
    let verifying_key = VerifyingKey::from_encoded_point(&point)
        .map_err(|_| WebAuthnError::InvalidSignature)?;

    let signature = Signature::from_der(sig_der)
        .map_err(|_| WebAuthnError::InvalidSignature)?;

    verifying_key.verify(data, &signature)
        .map_err(|_| WebAuthnError::InvalidSignature)
}

fn random_bytes(n: usize) -> Vec<u8> {
    use rand::RngCore;
    let mut bytes = vec![0u8; n];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn begin_authentication_produces_32_byte_challenge() {
        let config = WebAuthnConfig::new("example.com", "https://example.com");
        let (_opts, state) = begin_authentication(&config, "cred_id_123");
        assert_eq!(state.challenge_bytes.len(), 32);
    }

    #[test]
    fn begin_authentication_options_fields() {
        let config = WebAuthnConfig::new("example.com", "https://example.com");
        let (opts, _) = begin_authentication(&config, "cred_id_123");
        assert_eq!(opts.rp_id, "example.com");
        assert_eq!(opts.credential_id, "cred_id_123");
        assert_eq!(opts.timeout_ms, 60_000);
    }

    #[test]
    fn begin_authentication_challenge_is_base64url() {
        let config = WebAuthnConfig::new("example.com", "https://example.com");
        let (opts, state) = begin_authentication(&config, "cred");
        let decoded = b64url_decode(&opts.challenge).unwrap();
        assert_eq!(decoded, state.challenge_bytes);
    }

    #[test]
    fn begin_authentication_unique_challenges() {
        let config = WebAuthnConfig::new("example.com", "https://example.com");
        let (_, s1) = begin_authentication(&config, "cred");
        let (_, s2) = begin_authentication(&config, "cred");
        assert_ne!(s1.challenge_bytes, s2.challenge_bytes);
    }

    #[test]
    fn finish_authentication_wrong_type_fails() {
        let config = WebAuthnConfig::new("example.com", "https://example.com");
        let (_, ch) = begin_authentication(&config, "cred");

        // clientDataJSON with wrong type
        let cdj = serde_json::json!({
            "type": "webauthn.create",
            "challenge": b64url_encode(&ch.challenge_bytes),
            "origin": "https://example.com"
        });
        let cdj_bytes = serde_json::to_vec(&cdj).unwrap();

        let stored = StoredCredential {
            id: "cred".into(),
            public_key_cbor: vec![],
            sign_count: 0,
            user_id: "u1".into(),
        };

        let resp = AuthenticationResponse {
            id: "cred".into(),
            client_data_json: b64url_encode(&cdj_bytes),
            authenticator_data: b64url_encode(&[0u8; 37]),
            signature: b64url_encode(&[]),
        };

        let err = finish_authentication(&config, &ch, &resp, &stored).unwrap_err();
        assert_eq!(err, WebAuthnError::UnsupportedCredentialType);
    }

    #[test]
    fn finish_authentication_challenge_mismatch_fails() {
        let config = WebAuthnConfig::new("example.com", "https://example.com");
        let (_, ch) = begin_authentication(&config, "cred");

        let cdj = serde_json::json!({
            "type": "webauthn.get",
            "challenge": b64url_encode(&[0xffu8; 32]),  // wrong challenge
            "origin": "https://example.com"
        });
        let cdj_bytes = serde_json::to_vec(&cdj).unwrap();

        let stored = StoredCredential {
            id: "cred".into(),
            public_key_cbor: vec![],
            sign_count: 0,
            user_id: "u1".into(),
        };
        let resp = AuthenticationResponse {
            id: "cred".into(),
            client_data_json: b64url_encode(&cdj_bytes),
            authenticator_data: b64url_encode(&[0u8; 37]),
            signature: b64url_encode(&[]),
        };

        let err = finish_authentication(&config, &ch, &resp, &stored).unwrap_err();
        assert_eq!(err, WebAuthnError::ChallengeMismatch);
    }

    #[test]
    fn finish_authentication_origin_mismatch_fails() {
        let config = WebAuthnConfig::new("example.com", "https://example.com");
        let (_, ch) = begin_authentication(&config, "cred");

        let cdj = serde_json::json!({
            "type": "webauthn.get",
            "challenge": b64url_encode(&ch.challenge_bytes),
            "origin": "https://evil.com"
        });
        let cdj_bytes = serde_json::to_vec(&cdj).unwrap();

        let stored = StoredCredential {
            id: "cred".into(),
            public_key_cbor: vec![],
            sign_count: 0,
            user_id: "u1".into(),
        };
        let resp = AuthenticationResponse {
            id: "cred".into(),
            client_data_json: b64url_encode(&cdj_bytes),
            authenticator_data: b64url_encode(&[0u8; 37]),
            signature: b64url_encode(&[]),
        };

        let err = finish_authentication(&config, &ch, &resp, &stored).unwrap_err();
        assert_eq!(err, WebAuthnError::OriginMismatch);
    }

    #[test]
    fn finish_registration_challenge_mismatch_fails() {
        use crate::registration::{finish_registration, RegistrationChallenge, RegistrationResponse};
        let config = WebAuthnConfig::new("example.com", "https://example.com");

        let challenge = RegistrationChallenge {
            challenge_bytes: vec![1u8; 32],
            user_id: "u1".into(),
        };

        let cdj = serde_json::json!({
            "type": "webauthn.create",
            "challenge": b64url_encode(&[2u8; 32]),  // wrong
            "origin": "https://example.com"
        });
        let cdj_bytes = serde_json::to_vec(&cdj).unwrap();

        let resp = RegistrationResponse {
            id: "cred".into(),
            client_data_json: b64url_encode(&cdj_bytes),
            attestation_object: b64url_encode(&[]),
        };

        let err = finish_registration(&config, &challenge, &resp).unwrap_err();
        assert_eq!(err, WebAuthnError::ChallengeMismatch);
    }

    #[test]
    fn p256_signature_verification_with_generated_key() {
        use p256::ecdsa::{SigningKey, signature::Signer};
        use rand::rngs::OsRng;

        let signing_key = SigningKey::random(&mut OsRng);
        let verifying_key = signing_key.verifying_key();
        let point = verifying_key.to_encoded_point(false);
        let x = point.x().unwrap().to_vec();
        let y = point.y().unwrap().to_vec();

        // Build COSE key bytes matching the cbor::build_cose_key format
        let mut cose_bytes = vec![];
        cose_bytes.push(0xa5); // map(5)
        cose_bytes.push(0x01); cose_bytes.push(0x02); // 1:2
        cose_bytes.push(0x03); cose_bytes.push(0x26); // 3:-7
        cose_bytes.push(0x20); cose_bytes.push(0x01); // -1:1
        cose_bytes.push(0x21); cose_bytes.push(0x58); cose_bytes.push(32); cose_bytes.extend_from_slice(&x); // -2: x
        cose_bytes.push(0x22); cose_bytes.push(0x58); cose_bytes.push(32); cose_bytes.extend_from_slice(&y); // -3: y

        let data = b"test signing data";
        let sig: p256::ecdsa::Signature = signing_key.sign(data);
        let sig_der = sig.to_der().as_bytes().to_vec();

        assert!(verify_es256_signature(&cose_bytes, data, &sig_der).is_ok());
    }

    #[test]
    fn p256_wrong_signature_fails() {
        use p256::ecdsa::{SigningKey, signature::Signer};
        use rand::rngs::OsRng;

        let signing_key = SigningKey::random(&mut OsRng);
        let verifying_key = signing_key.verifying_key();
        let point = verifying_key.to_encoded_point(false);
        let x = point.x().unwrap().to_vec();
        let y = point.y().unwrap().to_vec();

        let mut cose_bytes = vec![];
        cose_bytes.push(0xa5);
        cose_bytes.push(0x01); cose_bytes.push(0x02);
        cose_bytes.push(0x03); cose_bytes.push(0x26);
        cose_bytes.push(0x20); cose_bytes.push(0x01);
        cose_bytes.push(0x21); cose_bytes.push(0x58); cose_bytes.push(32); cose_bytes.extend_from_slice(&x);
        cose_bytes.push(0x22); cose_bytes.push(0x58); cose_bytes.push(32); cose_bytes.extend_from_slice(&y);

        let data = b"real data";
        let sig: p256::ecdsa::Signature = signing_key.sign(data);
        let sig_der = sig.to_der().as_bytes().to_vec();

        // Verify against WRONG data
        let err = verify_es256_signature(&cose_bytes, b"different data", &sig_der).unwrap_err();
        assert_eq!(err, WebAuthnError::InvalidSignature);
    }
}
