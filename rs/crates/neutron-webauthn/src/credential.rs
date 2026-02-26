use serde::{Deserialize, Serialize};

/// A credential returned by the authenticator during registration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicKeyCredential {
    /// Base64URL-encoded credential ID.
    pub id: String,
    /// CBOR-encoded public key (stored for signature verification later).
    pub public_key_cbor: Vec<u8>,
    /// Signature counter at registration time.
    pub sign_count: u32,
    /// True if the authenticator is a platform authenticator (TouchID, etc.).
    pub is_platform: bool,
}

/// A credential stored after successful registration, used to verify future assertions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredCredential {
    /// Base64URL-encoded credential ID.
    pub id: String,
    /// CBOR-encoded COSE public key bytes.
    pub public_key_cbor: Vec<u8>,
    /// Last seen signature counter (used to detect cloned authenticators).
    pub sign_count: u32,
    /// User handle associated with this credential.
    pub user_id: String,
}

impl StoredCredential {
    pub fn from_registration(cred: PublicKeyCredential, user_id: impl Into<String>) -> Self {
        StoredCredential {
            id: cred.id,
            public_key_cbor: cred.public_key_cbor,
            sign_count: cred.sign_count,
            user_id: user_id.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_registration_copies_fields() {
        let cred = PublicKeyCredential {
            id: "cred_id".to_string(),
            public_key_cbor: vec![1, 2, 3],
            sign_count: 5,
            is_platform: true,
        };
        let stored = StoredCredential::from_registration(cred, "user_42");
        assert_eq!(stored.id, "cred_id");
        assert_eq!(stored.public_key_cbor, vec![1, 2, 3]);
        assert_eq!(stored.sign_count, 5);
        assert_eq!(stored.user_id, "user_42");
    }

    #[test]
    fn stored_credential_serializes() {
        let sc = StoredCredential {
            id: "abc".to_string(),
            public_key_cbor: vec![],
            sign_count: 0,
            user_id: "u1".to_string(),
        };
        let json = serde_json::to_string(&sc).unwrap();
        assert!(json.contains("abc"));
    }
}
