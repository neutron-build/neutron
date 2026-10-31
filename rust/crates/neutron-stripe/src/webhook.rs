//! Stripe webhook signature verification (Stripe-Signature header).
//!
//! Stripe signs each webhook with HMAC-SHA256 using the signing secret.
//! The `Stripe-Signature` header looks like:
//! ```text
//! t=1492774577,v1=5257a869e7ecebeda32affa62cdca3fa51cad7e77a05bd539e6966a5e46b3f5d,...
//! ```

use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::config::StripeConfig;
use crate::error::StripeError;
use crate::event::StripeEvent;

type HmacSha256 = Hmac<Sha256>;

/// Maximum age (seconds) of a Stripe webhook before it's considered stale.
pub const WEBHOOK_TOLERANCE_SECS: u64 = 300;

/// Verify the `Stripe-Signature` header and parse the event body.
///
/// Returns a parsed [`StripeEvent`] if the signature is valid and the
/// timestamp is within [`WEBHOOK_TOLERANCE_SECS`] of `now_secs`.
///
/// # Arguments
/// * `config`    – Stripe configuration holding the webhook secret.
/// * `payload`   – Raw request body bytes (do NOT parse before calling this).
/// * `sig_header`– Value of the `Stripe-Signature` HTTP header.
/// * `now_secs`  – Current Unix timestamp in seconds (pass `0` to skip replay check).
pub fn verify_webhook_signature(
    config: &StripeConfig,
    payload: &[u8],
    sig_header: &str,
    now_secs: u64,
) -> Result<StripeEvent, StripeError> {
    let (timestamp, v1_sigs) = parse_sig_header(sig_header)?;

    // Replay-attack guard
    if now_secs > 0 && now_secs.saturating_sub(timestamp) > WEBHOOK_TOLERANCE_SECS {
        return Err(StripeError::InvalidSignature(
            "webhook timestamp too old".to_string(),
        ));
    }

    if config.webhook_secret.is_empty() {
        return Err(StripeError::Config(
            "webhook secret is empty; refusing to verify against an unconfigured key".to_string(),
        ));
    }

    // Compute expected HMAC the way Stripe's official SDKs do:
    // HMAC-SHA256(secret, "<timestamp>.<payload>") where `secret` is the FULL
    // signing-secret string — including the `whsec_` prefix — used directly as
    // key bytes. It is NOT base64-decoded: an earlier version stripped the
    // prefix and decoded the remainder, which computed a different MAC than
    // every official signer, so real Stripe signatures never verified.
    // The signed payload is fed as timestamp bytes + "." + raw payload bytes
    // with no UTF-8 round-trip.
    let mut mac = HmacSha256::new_from_slice(config.webhook_secret.as_bytes())
        .map_err(|_| StripeError::Config("invalid webhook secret".to_string()))?;
    mac.update(timestamp.to_string().as_bytes());
    mac.update(b".");
    mac.update(payload);
    let expected = mac.finalize().into_bytes();
    let expected_hex = hex_encode(&expected);

    // Constant-time comparison against all v1 signatures in the header
    let matched = v1_sigs
        .iter()
        .any(|sig| constant_time_eq(sig, &expected_hex));
    if !matched {
        return Err(StripeError::InvalidSignature("HMAC mismatch".to_string()));
    }

    serde_json::from_slice(payload).map_err(|e| StripeError::ParseError(e.to_string()))
}

/// Parse the Stripe-Signature header into `(timestamp, vec_of_v1_signatures)`.
fn parse_sig_header(header: &str) -> Result<(u64, Vec<String>), StripeError> {
    let mut timestamp: Option<u64> = None;
    let mut v1_sigs: Vec<String> = Vec::new();

    for part in header.split(',') {
        if let Some(ts) = part.strip_prefix("t=") {
            timestamp =
                Some(ts.parse().map_err(|_| {
                    StripeError::InvalidSignature(format!("invalid timestamp: {ts}"))
                })?);
        } else if let Some(sig) = part.strip_prefix("v1=") {
            v1_sigs.push(sig.to_string());
        }
    }

    let ts = timestamp.ok_or_else(|| {
        StripeError::InvalidSignature("missing t= field in Stripe-Signature".to_string())
    })?;

    if v1_sigs.is_empty() {
        return Err(StripeError::InvalidSignature(
            "no v1= signatures in Stripe-Signature".to_string(),
        ));
    }

    Ok((ts, v1_sigs))
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Constant-time string comparison (avoids timing attacks).
fn constant_time_eq(a: &str, b: &str) -> bool {
    let a = a.as_bytes();
    let b = b.as_bytes();
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b.iter())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a Stripe-Signature header exactly the way Stripe's official
    /// signers do: HMAC-SHA256 over `"{t}.{payload}"` keyed by the FULL
    /// signing-secret string bytes (`whsec_` prefix included, never decoded).
    fn build_sig_header(secret: &str, payload: &[u8], timestamp: u64) -> String {
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(timestamp.to_string().as_bytes());
        mac.update(b".");
        mac.update(payload);
        let sig = hex_encode(&mac.finalize().into_bytes());
        format!("t={timestamp},v1={sig}")
    }

    const SECRET: &str = "whsec_dGVzdHNlY3JldA=="; // base64("testsecret")

    fn valid_payload() -> &'static [u8] {
        b"{\"id\":\"evt_1\",\"type\":\"payment_intent.created\",\"livemode\":false,\"created\":1700000000,\"data\":{\"object\":{}}}"
    }

    /// Regression (RS-13): a signature generated by an INDEPENDENT
    /// implementation (Python `hmac`, the same algorithm stripe-node uses —
    /// full secret string as key, `"{t}.{payload}"` as message) must verify.
    /// The pre-fix code stripped `whsec_` and base64-decoded the remainder, so
    /// this official-semantics signature failed while only self-generated
    /// (circular) fixtures passed.
    #[test]
    fn independently_generated_official_signature_verifies() {
        let payload = valid_payload();
        let ts = 1700000000u64;
        // python3: hmac.new(b'whsec_dGVzdHNlY3JldA==', str(ts).encode()+b'.'+payload, hashlib.sha256).hexdigest()
        let header =
            format!("t={ts},v1=02f0878d7942e3bbd248d569bae82e5f651e660f5c53812e19340d90707c0edf");
        let event =
            verify_webhook_signature(&StripeConfig::new(SECRET, "sk_test"), payload, &header, ts)
                .expect("official-semantics signature must verify");
        assert_eq!(event.id, "evt_1");
    }

    /// The OLD (wrong) transform — strip `whsec_`, base64-decode the rest,
    /// HMAC with the decoded bytes — must NOT verify against the fixed
    /// implementation.
    #[test]
    fn decoded_key_fixture_fails() {
        let payload = valid_payload();
        let ts = 1700000000u64;
        use base64::Engine;
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(SECRET.strip_prefix("whsec_").unwrap())
            .unwrap();
        let mut mac = HmacSha256::new_from_slice(&decoded).unwrap();
        mac.update(format!("{ts}.").as_bytes());
        mac.update(payload);
        let old_sig = hex_encode(&mac.finalize().into_bytes());
        let header = format!("t={ts},v1={old_sig}");
        let err =
            verify_webhook_signature(&StripeConfig::new(SECRET, "sk_test"), payload, &header, ts)
                .unwrap_err();
        assert!(matches!(err, StripeError::InvalidSignature(_)));
    }

    #[test]
    fn empty_webhook_secret_is_refused() {
        let err = verify_webhook_signature(
            &StripeConfig::new("", "sk_test"),
            valid_payload(),
            "t=1700000000,v1=00",
            1700000000,
        )
        .unwrap_err();
        assert!(matches!(err, StripeError::Config(_)));
    }

    #[test]
    fn valid_signature_parses_event() {
        let payload = valid_payload();
        let ts = 1700000000u64;
        let header = build_sig_header(SECRET, payload, ts);
        let event =
            verify_webhook_signature(&StripeConfig::new(SECRET, "sk_test"), payload, &header, ts)
                .unwrap();
        assert_eq!(event.id, "evt_1");
    }

    #[test]
    fn wrong_signature_fails() {
        let payload = valid_payload();
        let ts = 1700000000u64;
        let header =
            format!("t={ts},v1=deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef");
        let err =
            verify_webhook_signature(&StripeConfig::new(SECRET, "sk_test"), payload, &header, ts)
                .unwrap_err();
        assert!(matches!(err, StripeError::InvalidSignature(_)));
    }

    #[test]
    fn stale_timestamp_fails() {
        let payload = valid_payload();
        let ts = 1700000000u64;
        let header = build_sig_header(SECRET, payload, ts);
        let now = ts + WEBHOOK_TOLERANCE_SECS + 1;
        let err =
            verify_webhook_signature(&StripeConfig::new(SECRET, "sk_test"), payload, &header, now)
                .unwrap_err();
        assert!(matches!(err, StripeError::InvalidSignature(_)));
    }

    #[test]
    fn skip_replay_check_when_now_is_zero() {
        let payload = valid_payload();
        let ts = 1700000000u64;
        let header = build_sig_header(SECRET, payload, ts);
        // now_secs=0 means skip age check
        assert!(verify_webhook_signature(
            &StripeConfig::new(SECRET, "sk_test"),
            payload,
            &header,
            0,
        )
        .is_ok());
    }

    #[test]
    fn missing_timestamp_fails() {
        let err = parse_sig_header("v1=abc123").unwrap_err();
        assert!(matches!(err, StripeError::InvalidSignature(_)));
    }

    #[test]
    fn missing_v1_fails() {
        let err = parse_sig_header("t=12345").unwrap_err();
        assert!(matches!(err, StripeError::InvalidSignature(_)));
    }

    #[test]
    fn multiple_v1_any_match() {
        let payload = valid_payload();
        let ts = 1700000000u64;
        let real_header = build_sig_header(SECRET, payload, ts);
        // Extract the real v1 sig and prepend a fake one
        let real_v1 = real_header.split(',').nth(1).unwrap();
        let header = format!(
            "t={ts},v1=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,{real_v1}"
        );
        assert!(verify_webhook_signature(
            &StripeConfig::new(SECRET, "sk_test"),
            payload,
            &header,
            ts,
        )
        .is_ok());
    }

    #[test]
    fn constant_time_eq_same() {
        assert!(constant_time_eq("abc", "abc"));
    }

    #[test]
    fn constant_time_eq_diff() {
        assert!(!constant_time_eq("abc", "abd"));
    }

    #[test]
    fn constant_time_eq_different_lengths() {
        assert!(!constant_time_eq("ab", "abc"));
    }

    #[test]
    fn hex_encode_known() {
        assert_eq!(hex_encode(&[0xde, 0xad, 0xbe, 0xef]), "deadbeef");
    }
}
