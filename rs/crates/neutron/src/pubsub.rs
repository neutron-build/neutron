//! In-process PubSub system for real-time messaging.
//!
//! Provides a topic-based publish/subscribe broker built on
//! `tokio::sync::broadcast` channels. Designed for real-time features
//! like chat rooms, live updates, and notifications.
//!
//! # Example
//!
//! ```rust,ignore
//! use neutron::prelude::*;
//! use neutron::pubsub::PubSub;
//!
//! let pubsub = PubSub::new();
//!
//! let router = Router::new()
//!     .state(pubsub.clone())
//!     .post("/notify", |State(ps): State<PubSub>| async move {
//!         ps.publish("alerts", "New alert!");
//!         "sent"
//!     })
//!     .get("/listen", |State(ps): State<PubSub>| async move {
//!         let mut rx = ps.subscribe::<String>("alerts");
//!         let msg = rx.recv().await.unwrap();
//!         msg
//!     });
//! ```
//!
//! ## Integration with SSE
//!
//! ```rust,ignore
//! use neutron::sse::{Sse, SseEvent};
//!
//! async fn events(State(ps): State<PubSub>) -> Sse {
//!     let mut rx = ps.subscribe::<String>("updates");
//!     Sse::new(async_stream::stream! {
//!         while let Ok(msg) = rx.recv().await {
//!             yield SseEvent::new().data(&msg);
//!         }
//!     })
//! }
//! ```

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::broadcast;

// ---------------------------------------------------------------------------
// PubSub broker
// ---------------------------------------------------------------------------

/// In-process publish/subscribe broker.
///
/// Cheap to clone — all clones share the same underlying state.
///
/// Register as application state to use in handlers:
///
/// ```rust,ignore
/// let pubsub = PubSub::new();
/// Router::new().state(pubsub.clone());
/// ```
#[derive(Clone)]
pub struct PubSub {
    inner: Arc<PubSubInner>,
}

struct PubSubInner {
    /// JSON-based topics — messages serialized to String
    topics: Mutex<HashMap<String, broadcast::Sender<String>>>,
    /// Presence tracking: topic → set of subscriber IDs
    presence: Mutex<HashMap<String, HashSet<String>>>,
    /// Default channel capacity
    capacity: usize,
}

impl PubSub {
    /// Create a new PubSub broker with default capacity (256 messages).
    pub fn new() -> Self {
        Self::with_capacity(256)
    }

    /// Create a new PubSub broker with the given per-topic channel capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: Arc::new(PubSubInner {
                topics: Mutex::new(HashMap::new()),
                presence: Mutex::new(HashMap::new()),
                capacity,
            }),
        }
    }

    // -----------------------------------------------------------------------
    // JSON-based pub/sub (messages serialized as JSON strings)
    // -----------------------------------------------------------------------

    /// Publish a serializable message to a topic.
    ///
    /// Messages are serialized to JSON. Returns the number of active
    /// receivers that will receive the message, or 0 if no subscribers.
    pub fn publish<T: Serialize>(&self, topic: &str, message: &T) -> usize {
        let json = match serde_json::to_string(message) {
            Ok(j) => j,
            Err(_) => return 0,
        };

        let topics = self.inner.topics.lock().unwrap();
        if let Some(tx) = topics.get(topic) {
            tx.send(json).unwrap_or(0)
        } else {
            0
        }
    }

    /// Subscribe to a topic, receiving JSON-deserialized messages.
    ///
    /// Returns a [`Subscriber`] that yields messages of type `T`.
    pub fn subscribe<T: DeserializeOwned + Send + 'static>(&self, topic: &str) -> Subscriber<T> {
        let rx = {
            let mut topics = self.inner.topics.lock().unwrap();
            let tx = topics
                .entry(topic.to_string())
                .or_insert_with(|| broadcast::channel(self.inner.capacity).0);
            tx.subscribe()
        };

        Subscriber {
            rx,
            _marker: std::marker::PhantomData,
        }
    }

    /// Subscribe to a topic, receiving raw JSON strings.
    pub fn subscribe_raw(&self, topic: &str) -> broadcast::Receiver<String> {
        let mut topics = self.inner.topics.lock().unwrap();
        let tx = topics
            .entry(topic.to_string())
            .or_insert_with(|| broadcast::channel(self.inner.capacity).0);
        tx.subscribe()
    }

    /// Publish a raw JSON string to a topic.
    pub fn publish_raw(&self, topic: &str, json: String) -> usize {
        let topics = self.inner.topics.lock().unwrap();
        if let Some(tx) = topics.get(topic) {
            tx.send(json).unwrap_or(0)
        } else {
            0
        }
    }

    // -----------------------------------------------------------------------
    // Presence tracking
    // -----------------------------------------------------------------------

    /// Track a subscriber joining a topic.
    pub fn track(&self, topic: &str, id: impl Into<String>) {
        let mut presence = self.inner.presence.lock().unwrap();
        presence
            .entry(topic.to_string())
            .or_default()
            .insert(id.into());
    }

    /// Remove a subscriber from a topic's presence list.
    pub fn untrack(&self, topic: &str, id: &str) {
        let mut presence = self.inner.presence.lock().unwrap();
        if let Some(members) = presence.get_mut(topic) {
            members.remove(id);
            if members.is_empty() {
                presence.remove(topic);
            }
        }
    }

    /// Get the set of subscriber IDs for a topic.
    pub fn list(&self, topic: &str) -> HashSet<String> {
        let presence = self.inner.presence.lock().unwrap();
        presence.get(topic).cloned().unwrap_or_default()
    }

    /// Number of tracked subscribers for a topic.
    pub fn count(&self, topic: &str) -> usize {
        let presence = self.inner.presence.lock().unwrap();
        presence.get(topic).map(|s| s.len()).unwrap_or(0)
    }

    // -----------------------------------------------------------------------
    // Topic management
    // -----------------------------------------------------------------------

    /// Get the number of active topics.
    pub fn topic_count(&self) -> usize {
        self.inner.topics.lock().unwrap().len()
    }

    /// Check if a topic has any active subscribers.
    pub fn has_subscribers(&self, topic: &str) -> bool {
        let topics = self.inner.topics.lock().unwrap();
        topics
            .get(topic)
            .map(|tx| tx.receiver_count() > 0)
            .unwrap_or(false)
    }

    /// Get the number of active subscribers for a topic.
    pub fn subscriber_count(&self, topic: &str) -> usize {
        let topics = self.inner.topics.lock().unwrap();
        topics.get(topic).map(|tx| tx.receiver_count()).unwrap_or(0)
    }
}

impl Default for PubSub {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Subscriber
// ---------------------------------------------------------------------------

/// A typed subscriber that deserializes messages from JSON.
pub struct Subscriber<T> {
    rx: broadcast::Receiver<String>,
    _marker: std::marker::PhantomData<T>,
}

impl<T: DeserializeOwned> Subscriber<T> {
    /// Receive the next message, blocking until one is available.
    pub async fn recv(&mut self) -> Result<T, SubscriberError> {
        match self.rx.recv().await {
            Ok(json) => match serde_json::from_str(&json) {
                Ok(value) => Ok(value),
                Err(e) => Err(SubscriberError::Deserialize(e.to_string())),
            },
            Err(broadcast::error::RecvError::Lagged(n)) => Err(SubscriberError::Lagged(n)),
            Err(broadcast::error::RecvError::Closed) => Err(SubscriberError::Closed),
        }
    }

    /// Try to receive a message without blocking.
    pub fn try_recv(&mut self) -> Result<Option<T>, SubscriberError> {
        match self.rx.try_recv() {
            Ok(json) => match serde_json::from_str(&json) {
                Ok(value) => Ok(Some(value)),
                Err(e) => Err(SubscriberError::Deserialize(e.to_string())),
            },
            Err(broadcast::error::TryRecvError::Empty) => Ok(None),
            Err(broadcast::error::TryRecvError::Lagged(n)) => Err(SubscriberError::Lagged(n)),
            Err(broadcast::error::TryRecvError::Closed) => Err(SubscriberError::Closed),
        }
    }
}

// ---------------------------------------------------------------------------
// SubscriberError
// ---------------------------------------------------------------------------

/// Errors from receiving messages.
#[derive(Debug)]
pub enum SubscriberError {
    /// Channel was closed (all senders dropped).
    Closed,
    /// Receiver fell behind and missed `n` messages.
    Lagged(u64),
    /// Failed to deserialize the message.
    Deserialize(String),
}

impl std::fmt::Display for SubscriberError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Closed => write!(f, "channel closed"),
            Self::Lagged(n) => write!(f, "lagged behind by {n} messages"),
            Self::Deserialize(e) => write!(f, "deserialization error: {e}"),
        }
    }
}

impl std::error::Error for SubscriberError {}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
    struct ChatMessage {
        user: String,
        text: String,
    }

    #[tokio::test]
    async fn publish_and_subscribe() {
        let ps = PubSub::new();
        let mut rx = ps.subscribe::<String>("greetings");

        ps.publish("greetings", &"hello".to_string());
        let msg = rx.recv().await.unwrap();
        assert_eq!(msg, "hello");
    }

    #[tokio::test]
    async fn subscribe_typed_message() {
        let ps = PubSub::new();
        let mut rx = ps.subscribe::<ChatMessage>("chat");

        let msg = ChatMessage {
            user: "Alice".into(),
            text: "Hi!".into(),
        };
        ps.publish("chat", &msg);

        let received = rx.recv().await.unwrap();
        assert_eq!(received, msg);
    }

    #[tokio::test]
    async fn multiple_subscribers() {
        let ps = PubSub::new();
        let mut rx1 = ps.subscribe::<String>("topic");
        let mut rx2 = ps.subscribe::<String>("topic");

        ps.publish("topic", &"broadcast".to_string());

        assert_eq!(rx1.recv().await.unwrap(), "broadcast");
        assert_eq!(rx2.recv().await.unwrap(), "broadcast");
    }

    #[tokio::test]
    async fn publish_to_empty_topic_returns_zero() {
        let ps = PubSub::new();
        let count = ps.publish("empty", &"no one listening".to_string());
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn publish_returns_subscriber_count() {
        let ps = PubSub::new();
        let _rx1 = ps.subscribe::<String>("topic");
        let _rx2 = ps.subscribe::<String>("topic");

        let count = ps.publish("topic", &"msg".to_string());
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn raw_publish_and_subscribe() {
        let ps = PubSub::new();
        let mut rx = ps.subscribe_raw("raw");

        ps.publish_raw("raw", r#""hello""#.to_string());
        let msg = rx.recv().await.unwrap();
        assert_eq!(msg, r#""hello""#);
    }

    #[tokio::test]
    async fn different_topics_independent() {
        let ps = PubSub::new();
        let mut rx_a = ps.subscribe::<String>("a");
        let mut rx_b = ps.subscribe::<String>("b");

        ps.publish("a", &"msg-a".to_string());

        assert_eq!(rx_a.recv().await.unwrap(), "msg-a");

        // rx_b should have nothing
        assert!(rx_b.try_recv().unwrap().is_none());
    }

    #[tokio::test]
    async fn try_recv_empty() {
        let ps = PubSub::new();
        let mut rx = ps.subscribe::<String>("empty");

        assert!(rx.try_recv().unwrap().is_none());
    }

    #[tokio::test]
    async fn try_recv_with_message() {
        let ps = PubSub::new();
        let mut rx = ps.subscribe::<String>("topic");

        ps.publish("topic", &"msg".to_string());

        let result = rx.try_recv().unwrap();
        assert_eq!(result, Some("msg".to_string()));
    }

    // -----------------------------------------------------------------------
    // Presence tracking
    // -----------------------------------------------------------------------

    #[test]
    fn track_and_list() {
        let ps = PubSub::new();

        ps.track("room:1", "user-1");
        ps.track("room:1", "user-2");

        let members = ps.list("room:1");
        assert_eq!(members.len(), 2);
        assert!(members.contains("user-1"));
        assert!(members.contains("user-2"));
    }

    #[test]
    fn untrack_removes_member() {
        let ps = PubSub::new();

        ps.track("room:1", "user-1");
        ps.track("room:1", "user-2");
        ps.untrack("room:1", "user-1");

        let members = ps.list("room:1");
        assert_eq!(members.len(), 1);
        assert!(members.contains("user-2"));
    }

    #[test]
    fn untrack_cleans_empty_topic() {
        let ps = PubSub::new();

        ps.track("room:1", "user-1");
        ps.untrack("room:1", "user-1");

        assert_eq!(ps.count("room:1"), 0);
        assert!(ps.list("room:1").is_empty());
    }

    #[test]
    fn count_returns_presence_count() {
        let ps = PubSub::new();

        assert_eq!(ps.count("room"), 0);
        ps.track("room", "a");
        assert_eq!(ps.count("room"), 1);
        ps.track("room", "b");
        assert_eq!(ps.count("room"), 2);
    }

    #[test]
    fn list_empty_topic() {
        let ps = PubSub::new();
        assert!(ps.list("nonexistent").is_empty());
    }

    // -----------------------------------------------------------------------
    // Topic management
    // -----------------------------------------------------------------------

    #[test]
    fn topic_count() {
        let ps = PubSub::new();
        assert_eq!(ps.topic_count(), 0);

        let _rx = ps.subscribe::<String>("a");
        assert_eq!(ps.topic_count(), 1);

        let _rx = ps.subscribe::<String>("b");
        assert_eq!(ps.topic_count(), 2);
    }

    #[test]
    fn has_subscribers() {
        let ps = PubSub::new();
        assert!(!ps.has_subscribers("topic"));

        let rx = ps.subscribe::<String>("topic");
        assert!(ps.has_subscribers("topic"));

        drop(rx);
        assert!(!ps.has_subscribers("topic"));
    }

    #[test]
    fn subscriber_count() {
        let ps = PubSub::new();
        assert_eq!(ps.subscriber_count("topic"), 0);

        let _rx1 = ps.subscribe::<String>("topic");
        assert_eq!(ps.subscriber_count("topic"), 1);

        let _rx2 = ps.subscribe::<String>("topic");
        assert_eq!(ps.subscriber_count("topic"), 2);
    }

    // -----------------------------------------------------------------------
    // Clone shares state
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn clone_shares_state() {
        let ps1 = PubSub::new();
        let ps2 = ps1.clone();

        let mut rx = ps1.subscribe::<String>("shared");
        ps2.publish("shared", &"from clone".to_string());

        assert_eq!(rx.recv().await.unwrap(), "from clone");
    }

    // -----------------------------------------------------------------------
    // Integration test with handler
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn handler_integration() {
        use crate::extract::State;
        use crate::router::Router;
        use crate::testing::TestClient;
        use http::StatusCode;

        let ps = PubSub::new();
        let mut rx = ps.subscribe::<String>("events");

        let client = TestClient::new(
            Router::new()
                .state(ps.clone())
                .post("/emit", |State(ps): State<PubSub>| async move {
                    ps.publish("events", &"happened".to_string());
                    "emitted"
                }),
        );

        let resp = client.post("/emit").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.text().await, "emitted");

        let msg = rx.recv().await.unwrap();
        assert_eq!(msg, "happened");
    }
}
