//! Stripe webhooks and payment intents for Neutron.
//!
//! # Webhook signature verification
//!
//! ```rust,ignore
//! use neutron_stripe::{StripeConfig, verify_webhook_signature, parse_event};
//!
//! let config = StripeConfig::new("whsec_...", "sk_test_...");
//! let event  = verify_webhook_signature(&config, payload_bytes, stripe_signature_header)?;
//! ```
//!
//! # Creating a payment intent
//!
//! ```rust,ignore
//! use neutron_stripe::{StripeClient, CreatePaymentIntent};
//!
//! let client = StripeClient::new("sk_test_...");
//! let pi = client.create_payment_intent(CreatePaymentIntent {
//!     amount:   2000,     // in smallest currency unit (cents)
//!     currency: "usd".to_string(),
//!     ..Default::default()
//! }).await?;
//! println!("client_secret = {}", pi.client_secret.unwrap_or_default());
//! ```

pub mod client;
pub mod config;
pub mod error;
pub mod event;
pub mod webhook;

pub use client::{CreatePaymentIntent, StripeClient};
pub use config::StripeConfig;
pub use error::StripeError;
pub use event::{PaymentIntent, StripeEvent, StripeEventType};
pub use webhook::verify_webhook_signature;

pub mod prelude {
    pub use crate::{
        CreatePaymentIntent, StripeClient, StripeConfig, StripeError, StripeEvent,
        StripeEventType, verify_webhook_signature,
    };
}
