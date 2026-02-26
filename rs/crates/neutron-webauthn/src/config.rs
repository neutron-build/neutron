/// Configuration for a WebAuthn relying party.
#[derive(Debug, Clone)]
pub struct WebAuthnConfig {
    /// RP ID — typically the domain name, e.g. `"example.com"`.
    pub rp_id: String,
    /// Allowed origin, e.g. `"https://example.com"`.
    pub origin: String,
    /// Human-readable display name shown in authenticator dialogs.
    pub rp_name: String,
    /// Require user verification (biometrics / PIN).
    pub require_user_verification: bool,
}

impl WebAuthnConfig {
    pub fn new(rp_id: impl Into<String>, origin: impl Into<String>) -> Self {
        let id = rp_id.into();
        WebAuthnConfig {
            rp_name: id.clone(),
            rp_id: id,
            origin: origin.into(),
            require_user_verification: true,
        }
    }

    pub fn rp_name(mut self, name: impl Into<String>) -> Self {
        self.rp_name = name.into();
        self
    }

    pub fn require_user_verification(mut self, require: bool) -> Self {
        self.require_user_verification = require;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults() {
        let c = WebAuthnConfig::new("example.com", "https://example.com");
        assert_eq!(c.rp_id, "example.com");
        assert_eq!(c.origin, "https://example.com");
        assert!(c.require_user_verification);
    }

    #[test]
    fn rp_name_defaults_to_rp_id() {
        let c = WebAuthnConfig::new("example.com", "https://example.com");
        assert_eq!(c.rp_name, "example.com");
    }

    #[test]
    fn custom_rp_name() {
        let c = WebAuthnConfig::new("example.com", "https://example.com")
            .rp_name("Example Corp");
        assert_eq!(c.rp_name, "Example Corp");
    }

    #[test]
    fn disable_user_verification() {
        let c = WebAuthnConfig::new("example.com", "https://example.com")
            .require_user_verification(false);
        assert!(!c.require_user_verification);
    }
}
