use std::fmt;

/// Errors produced by neutron-stripe.
#[derive(Debug)]
pub enum StripeError {
    /// Webhook signature is missing, malformed, or invalid.
    InvalidSignature(String),
    /// JSON payload could not be deserialized.
    ParseError(String),
    /// HTTP request to the Stripe API failed.
    ApiError(String),
    /// The Stripe API returned a non-2xx status with an error body.
    StripeApiError { status: u16, message: String },
    /// Misconfigured client (empty key, etc.).
    Config(String),
}

impl fmt::Display for StripeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StripeError::InvalidSignature(m)     => write!(f, "stripe: invalid signature: {m}"),
            StripeError::ParseError(m)           => write!(f, "stripe: parse error: {m}"),
            StripeError::ApiError(m)             => write!(f, "stripe: API error: {m}"),
            StripeError::StripeApiError { status, message } =>
                write!(f, "stripe: {status} {message}"),
            StripeError::Config(m)               => write!(f, "stripe: config error: {m}"),
        }
    }
}

impl std::error::Error for StripeError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_invalid_signature() {
        let e = StripeError::InvalidSignature("bad".to_string());
        assert!(e.to_string().contains("invalid signature"));
    }

    #[test]
    fn display_api_error() {
        let e = StripeError::StripeApiError { status: 402, message: "card_declined".to_string() };
        assert!(e.to_string().contains("402"));
        assert!(e.to_string().contains("card_declined"));
    }
}
