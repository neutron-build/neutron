//! WebAuthn/Passkey authentication for Neutron.
//!
//! Implements the core of WebAuthn Level 2 — registration and authentication —
//! using P-256 ECDSA (the algorithm used by platform authenticators such as
//! Face ID, Touch ID, and Windows Hello).
//!
//! # Registration
//!
//! ```rust,ignore
//! use neutron_webauthn::{WebAuthnConfig, begin_registration, finish_registration};
//!
//! let config = WebAuthnConfig::new("example.com", "https://example.com");
//! let (options, challenge) = begin_registration(&config, user_id, username);
//! // … send options to browser, receive credential response …
//! let credential = finish_registration(&config, &challenge, &response)?;
//! // Store credential.id + credential.public_key_cbor for later authentication
//! ```
//!
//! # Authentication
//!
//! ```rust,ignore
//! use neutron_webauthn::{begin_authentication, finish_authentication};
//!
//! let (options, challenge) = begin_authentication(&config, &stored_credential_id);
//! // … send options to browser, receive assertion response …
//! finish_authentication(&config, &challenge, &assertion, &stored_public_key_cbor)?;
//! ```

pub mod cbor;
pub mod config;
pub mod credential;
pub mod error;
pub mod registration;
pub mod authentication;

pub use config::WebAuthnConfig;
pub use credential::{PublicKeyCredential, StoredCredential};
pub use error::WebAuthnError;
pub use registration::{
    begin_registration, finish_registration,
    RegistrationChallenge, RegistrationOptions, RegistrationResponse,
};
pub use authentication::{
    begin_authentication, finish_authentication,
    AuthenticationChallenge, AuthenticationOptions, AuthenticationResponse,
};

pub mod prelude {
    pub use crate::{
        WebAuthnConfig, WebAuthnError,
        begin_registration, finish_registration,
        begin_authentication, finish_authentication,
        StoredCredential,
    };
}
