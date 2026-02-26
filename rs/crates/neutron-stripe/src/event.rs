use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Known Stripe event types (non-exhaustive).
#[derive(Debug, Clone, PartialEq)]
pub enum StripeEventType {
    PaymentIntentSucceeded,
    PaymentIntentPaymentFailed,
    PaymentIntentCreated,
    PaymentIntentCanceled,
    CustomerCreated,
    CustomerDeleted,
    CheckoutSessionCompleted,
    /// Any event type not explicitly enumerated.
    Other(String),
}

impl std::str::FromStr for StripeEventType {
    type Err = std::convert::Infallible;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "payment_intent.succeeded"       => StripeEventType::PaymentIntentSucceeded,
            "payment_intent.payment_failed"  => StripeEventType::PaymentIntentPaymentFailed,
            "payment_intent.created"         => StripeEventType::PaymentIntentCreated,
            "payment_intent.canceled"        => StripeEventType::PaymentIntentCanceled,
            "customer.created"               => StripeEventType::CustomerCreated,
            "customer.deleted"               => StripeEventType::CustomerDeleted,
            "checkout.session.completed"     => StripeEventType::CheckoutSessionCompleted,
            other                            => StripeEventType::Other(other.to_string()),
        })
    }
}

impl StripeEventType {

    pub fn as_str(&self) -> &str {
        match self {
            StripeEventType::PaymentIntentSucceeded    => "payment_intent.succeeded",
            StripeEventType::PaymentIntentPaymentFailed => "payment_intent.payment_failed",
            StripeEventType::PaymentIntentCreated      => "payment_intent.created",
            StripeEventType::PaymentIntentCanceled     => "payment_intent.canceled",
            StripeEventType::CustomerCreated           => "customer.created",
            StripeEventType::CustomerDeleted           => "customer.deleted",
            StripeEventType::CheckoutSessionCompleted  => "checkout.session.completed",
            StripeEventType::Other(s)                  => s.as_str(),
        }
    }
}

/// A Stripe webhook event envelope.
#[derive(Debug, Clone, Deserialize)]
pub struct StripeEvent {
    pub id:         String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub livemode:   bool,
    pub created:    u64,
    pub data:       StripeEventData,
}

impl StripeEvent {
    /// Parse the event type into a [`StripeEventType`].
    pub fn kind(&self) -> StripeEventType {
        self.event_type.parse().unwrap()
    }
}

/// The `data.object` wrapper inside a Stripe event.
#[derive(Debug, Clone, Deserialize)]
pub struct StripeEventData {
    pub object: Value,
}

/// A Stripe PaymentIntent object.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PaymentIntent {
    pub id:            String,
    pub object:        String,
    /// Amount in smallest currency unit (e.g. cents).
    pub amount:        i64,
    pub currency:      String,
    pub status:        String,
    pub client_secret: Option<String>,
    pub customer:      Option<String>,
    pub description:   Option<String>,
    pub metadata:      Option<Value>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_event(kind: &str) -> StripeEvent {
        let raw = json!({
            "id": "evt_test",
            "type": kind,
            "livemode": false,
            "created": 1700000000u64,
            "data": { "object": {} }
        });
        serde_json::from_value(raw).unwrap()
    }

    #[test]
    fn parse_payment_intent_succeeded() {
        let ev = make_event("payment_intent.succeeded");
        assert_eq!(ev.kind(), StripeEventType::PaymentIntentSucceeded);
    }

    #[test]
    fn parse_checkout_session_completed() {
        let ev = make_event("checkout.session.completed");
        assert_eq!(ev.kind(), StripeEventType::CheckoutSessionCompleted);
    }

    #[test]
    fn parse_unknown_event() {
        let ev = make_event("invoice.paid");
        assert_eq!(ev.kind(), StripeEventType::Other("invoice.paid".to_string()));
    }

    #[test]
    fn event_type_roundtrip() {
        let kinds = [
            "payment_intent.succeeded",
            "payment_intent.payment_failed",
            "payment_intent.created",
            "payment_intent.canceled",
            "customer.created",
            "customer.deleted",
            "checkout.session.completed",
        ];
        for kind in &kinds {
            assert_eq!(kind.parse::<StripeEventType>().unwrap().as_str(), *kind);
        }
    }

    #[test]
    fn parse_payment_intent_object() {
        let raw = json!({
            "id": "pi_test",
            "object": "payment_intent",
            "amount": 2000,
            "currency": "usd",
            "status": "succeeded",
            "client_secret": "pi_test_secret",
            "customer": null,
            "description": null,
            "metadata": null
        });
        let pi: PaymentIntent = serde_json::from_value(raw).unwrap();
        assert_eq!(pi.id, "pi_test");
        assert_eq!(pi.amount, 2000);
        assert_eq!(pi.currency, "usd");
        assert_eq!(pi.client_secret.as_deref(), Some("pi_test_secret"));
    }

    #[test]
    fn event_livemode_false() {
        let ev = make_event("payment_intent.created");
        assert!(!ev.livemode);
    }

    #[test]
    fn event_created_timestamp() {
        let ev = make_event("payment_intent.created");
        assert_eq!(ev.created, 1700000000);
    }
}
