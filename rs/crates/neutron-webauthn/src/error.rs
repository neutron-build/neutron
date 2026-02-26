use std::fmt;

/// Errors produced by neutron-webauthn.
#[derive(Debug, PartialEq)]
pub enum WebAuthnError {
    /// Base64 decoding failed.
    Base64(String),
    /// CBOR decoding failed.
    Cbor(String),
    /// JSON parsing failed.
    Json(String),
    /// The challenge in the response does not match the stored challenge.
    ChallengeMismatch,
    /// The relying party ID / origin does not match.
    OriginMismatch,
    /// The authenticator data flags indicate the user was not verified.
    UserNotVerified,
    /// Signature verification failed.
    InvalidSignature,
    /// A required field is missing from the response.
    MissingField(String),
    /// The credential type is unsupported (must be `public-key`).
    UnsupportedCredentialType,
    /// The algorithm is unsupported (must be ES256 / COSE -7).
    UnsupportedAlgorithm,
}

impl fmt::Display for WebAuthnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WebAuthnError::Base64(m)          => write!(f, "webauthn: base64 error: {m}"),
            WebAuthnError::Cbor(m)            => write!(f, "webauthn: cbor error: {m}"),
            WebAuthnError::Json(m)            => write!(f, "webauthn: json error: {m}"),
            WebAuthnError::ChallengeMismatch  => write!(f, "webauthn: challenge mismatch"),
            WebAuthnError::OriginMismatch     => write!(f, "webauthn: origin mismatch"),
            WebAuthnError::UserNotVerified    => write!(f, "webauthn: user not verified (UV flag not set)"),
            WebAuthnError::InvalidSignature   => write!(f, "webauthn: invalid signature"),
            WebAuthnError::MissingField(m)    => write!(f, "webauthn: missing field: {m}"),
            WebAuthnError::UnsupportedCredentialType => write!(f, "webauthn: credential type must be public-key"),
            WebAuthnError::UnsupportedAlgorithm      => write!(f, "webauthn: algorithm must be ES256"),
        }
    }
}

impl std::error::Error for WebAuthnError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_challenge_mismatch() {
        assert!(WebAuthnError::ChallengeMismatch.to_string().contains("challenge mismatch"));
    }

    #[test]
    fn display_origin_mismatch() {
        assert!(WebAuthnError::OriginMismatch.to_string().contains("origin mismatch"));
    }

    #[test]
    fn display_missing_field() {
        let e = WebAuthnError::MissingField("id".to_string());
        assert!(e.to_string().contains("missing field"));
        assert!(e.to_string().contains("id"));
    }
}
