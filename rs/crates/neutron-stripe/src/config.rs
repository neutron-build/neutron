/// Configuration for a Stripe integration.
#[derive(Debug, Clone)]
pub struct StripeConfig {
    /// Webhook signing secret (starts with `whsec_`).
    pub webhook_secret: String,
    /// Secret API key (starts with `sk_`).
    pub secret_key: String,
    /// Stripe API base URL (override for testing).
    pub api_base: String,
}

impl StripeConfig {
    /// Create a config from a webhook secret and API key.
    pub fn new(
        webhook_secret: impl Into<String>,
        secret_key: impl Into<String>,
    ) -> Self {
        StripeConfig {
            webhook_secret: webhook_secret.into(),
            secret_key: secret_key.into(),
            api_base: "https://api.stripe.com".to_string(),
        }
    }

    /// Override the API base URL (useful for tests with a local mock server).
    pub fn api_base(mut self, base: impl Into<String>) -> Self {
        self.api_base = base.into();
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
        let c = StripeConfig::new("whsec_abc", "sk_test_xyz")
            .api_base("http://localhost:12111");
        assert_eq!(c.api_base, "http://localhost:12111");
    }

    #[test]
    fn stores_keys() {
        let c = StripeConfig::new("whsec_abc", "sk_test_xyz");
        assert_eq!(c.webhook_secret, "whsec_abc");
        assert_eq!(c.secret_key, "sk_test_xyz");
    }
}
