/// Configuration for a Stripe integration.
#[derive(Debug, Clone)]
pub struct StripeConfig {
    /// Webhook signing secret (starts with `whsec_`).
    pub webhook_secret: String,
    /// Secret API key (starts with `sk_`).
    pub secret_key: String,
    /// Stripe API base URL (override for testing).
    pub api_base: String,
    /// Explicit opt-in for plaintext `http` bases. The client refuses to
    /// send API credentials over an unencrypted connection otherwise; this
    /// exists only for local test doubles.
    pub allow_insecure_http: bool,
    pub operation_timeout: std::time::Duration,
    pub max_response_bytes: usize,
}

impl StripeConfig {
    /// Create a config from a webhook secret and API key.
    pub fn new(webhook_secret: impl Into<String>, secret_key: impl Into<String>) -> Self {
        StripeConfig {
            webhook_secret: webhook_secret.into(),
            secret_key: secret_key.into(),
            api_base: "https://api.stripe.com".to_string(),
            allow_insecure_http: false,
            operation_timeout: std::time::Duration::from_secs(30),
            max_response_bytes: 2 * 1024 * 1024,
        }
    }

    /// Deadline includes connect, TLS, headers, body and JSON parsing.
    pub fn operation_limits(mut self, timeout: std::time::Duration, max_bytes: usize) -> Self {
        self.operation_timeout = timeout;
        self.max_response_bytes = max_bytes;
        self
    }

    /// Override the API base URL (useful for tests with a local mock server).
    pub fn api_base(mut self, base: impl Into<String>) -> Self {
        self.api_base = base.into();
        self
    }

    /// Explicitly allow plaintext `http` transports (local test doubles
    /// only). Required together with an `http://` base; credentials are
    /// otherwise never sent unencrypted.
    pub fn allow_insecure_http(mut self, allow: bool) -> Self {
        self.allow_insecure_http = allow;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_api_base() {
        let c = StripeConfig::new("whsec_abc", "sk_test_xyz");
        assert_eq!(c.api_base, "https://api.stripe.com");
    }

    #[test]
    fn custom_api_base() {
        let c = StripeConfig::new("whsec_abc", "sk_test_xyz").api_base("http://localhost:12111");
        assert_eq!(c.api_base, "http://localhost:12111");
    }

    #[test]
    fn stores_keys() {
        let c = StripeConfig::new("whsec_abc", "sk_test_xyz");
        assert_eq!(c.webhook_secret, "whsec_abc");
        assert_eq!(c.secret_key, "sk_test_xyz");
    }
}
