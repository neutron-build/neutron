//! Shared Pub/Sub registry for RESP connections.
//!
//! Wraps the Nucleus `PubSubHub` with thread-safe access and provides a
//! channel-based subscription model suitable for the async RESP server loop.
//! Each subscriber gets a `tokio::sync::mpsc` sender/receiver pair so the
//! connection loop can `select!` between incoming commands and outgoing
//! subscription messages.

use std::collections::{HashMap, HashSet};

use parking_lot::Mutex;
use tokio::sync::mpsc;

/// A message delivered to a subscriber.
#[derive(Debug, Clone)]
pub struct SubMessage {
    /// The channel or pattern that matched.
    pub channel: String,
    /// The actual channel the message was published on (same as `channel` for
    /// direct subscriptions, different for pattern subscriptions).
    pub actual_channel: String,
    /// The message payload.
    pub payload: String,
    /// Whether this was a pattern match.
    pub is_pattern: bool,
}

/// Per-subscriber handle returned when subscribing. The RESP server loop reads
/// from `rx` to push messages to the client.
pub struct Subscription {
    pub rx: mpsc::UnboundedReceiver<SubMessage>,
}

/// Shared pub/sub state across all RESP connections.
pub struct PubSubRegistry {
    inner: Mutex<PubSubInner>,
}

struct PubSubInner {
    /// channel → set of subscriber IDs
    channels: HashMap<String, HashSet<u64>>,
    /// subscriber_id → sender
    senders: HashMap<u64, mpsc::UnboundedSender<SubMessage>>,
    /// subscriber_id → set of subscribed channels
    sub_channels: HashMap<u64, HashSet<String>>,
    /// pattern → set of subscriber IDs
    patterns: HashMap<String, HashSet<u64>>,
    /// subscriber_id → set of subscribed patterns
    sub_patterns: HashMap<u64, HashSet<String>>,
    next_id: u64,
}

impl PubSubRegistry {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(PubSubInner {
                channels: HashMap::new(),
                senders: HashMap::new(),
                sub_channels: HashMap::new(),
                patterns: HashMap::new(),
                sub_patterns: HashMap::new(),
                next_id: 1,
            }),
        }
    }

    /// Allocate a new subscriber ID and sender/receiver pair.
    pub fn new_subscriber(&self) -> (u64, Subscription) {
        let (tx, rx) = mpsc::unbounded_channel();
        let mut inner = self.inner.lock();
        let id = inner.next_id;
        inner.next_id += 1;
        inner.senders.insert(id, tx);
        inner.sub_channels.insert(id, HashSet::new());
        inner.sub_patterns.insert(id, HashSet::new());
        (id, Subscription { rx })
    }

    /// Subscribe a subscriber to a channel. Returns the total number of
    /// subscriptions (channels + patterns) for this subscriber.
    pub fn subscribe(&self, sub_id: u64, channel: &str) -> usize {
        let mut inner = self.inner.lock();
        inner
            .channels
            .entry(channel.to_string())
            .or_default()
            .insert(sub_id);
        inner
            .sub_channels
            .entry(sub_id)
            .or_default()
            .insert(channel.to_string());
        let ch_count = inner.sub_channels.get(&sub_id).map_or(0, |s| s.len());
        let pat_count = inner.sub_patterns.get(&sub_id).map_or(0, |s| s.len());
        ch_count + pat_count
    }

    /// Unsubscribe a subscriber from a channel. Returns the remaining total
    /// subscription count for this subscriber.
    pub fn unsubscribe(&self, sub_id: u64, channel: &str) -> usize {
        let mut inner = self.inner.lock();
        if let Some(subs) = inner.channels.get_mut(channel) {
            subs.remove(&sub_id);
            if subs.is_empty() {
                inner.channels.remove(channel);
            }
        }
        if let Some(chans) = inner.sub_channels.get_mut(&sub_id) {
            chans.remove(channel);
        }
        let ch_count = inner.sub_channels.get(&sub_id).map_or(0, |s| s.len());
        let pat_count = inner.sub_patterns.get(&sub_id).map_or(0, |s| s.len());
        ch_count + pat_count
    }

    /// Unsubscribe a subscriber from all channels. Returns a list of
    /// channels that were unsubscribed.
    pub fn unsubscribe_all(&self, sub_id: u64) -> Vec<String> {
        let mut inner = self.inner.lock();
        let channels: Vec<String> = inner
            .sub_channels
            .get(&sub_id)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default();
        for ch in &channels {
            if let Some(subs) = inner.channels.get_mut(ch) {
                subs.remove(&sub_id);
                if subs.is_empty() {
                    inner.channels.remove(ch);
                }
            }
        }
        if let Some(chans) = inner.sub_channels.get_mut(&sub_id) {
            chans.clear();
        }
        channels
    }

    /// Subscribe to a pattern (glob-style). Returns total subscription count.
    pub fn psubscribe(&self, sub_id: u64, pattern: &str) -> usize {
        let mut inner = self.inner.lock();
        inner
            .patterns
            .entry(pattern.to_string())
            .or_default()
            .insert(sub_id);
        inner
            .sub_patterns
            .entry(sub_id)
            .or_default()
            .insert(pattern.to_string());
        let ch_count = inner.sub_channels.get(&sub_id).map_or(0, |s| s.len());
        let pat_count = inner.sub_patterns.get(&sub_id).map_or(0, |s| s.len());
        ch_count + pat_count
    }

    /// Unsubscribe from a pattern. Returns remaining total subscription count.
    pub fn punsubscribe(&self, sub_id: u64, pattern: &str) -> usize {
        let mut inner = self.inner.lock();
        if let Some(subs) = inner.patterns.get_mut(pattern) {
            subs.remove(&sub_id);
            if subs.is_empty() {
                inner.patterns.remove(pattern);
            }
        }
        if let Some(pats) = inner.sub_patterns.get_mut(&sub_id) {
            pats.remove(pattern);
        }
        let ch_count = inner.sub_channels.get(&sub_id).map_or(0, |s| s.len());
        let pat_count = inner.sub_patterns.get(&sub_id).map_or(0, |s| s.len());
        ch_count + pat_count
    }

    /// Unsubscribe from all patterns. Returns a list of patterns unsubscribed.
    pub fn punsubscribe_all(&self, sub_id: u64) -> Vec<String> {
        let mut inner = self.inner.lock();
        let patterns: Vec<String> = inner
            .sub_patterns
            .get(&sub_id)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default();
        for pat in &patterns {
            if let Some(subs) = inner.patterns.get_mut(pat) {
                subs.remove(&sub_id);
                if subs.is_empty() {
                    inner.patterns.remove(pat);
                }
            }
        }
        if let Some(pats) = inner.sub_patterns.get_mut(&sub_id) {
            pats.clear();
        }
        patterns
    }

    /// Publish a message to a channel. Returns the number of subscribers that
    /// received it (direct + pattern).
    pub fn publish(&self, channel: &str, message: &str) -> usize {
        let inner = self.inner.lock();
        let mut count = 0;

        // Direct subscribers
        if let Some(subs) = inner.channels.get(channel) {
            for &sub_id in subs {
                if let Some(tx) = inner.senders.get(&sub_id) {
                    let msg = SubMessage {
                        channel: channel.to_string(),
                        actual_channel: channel.to_string(),
                        payload: message.to_string(),
                        is_pattern: false,
                    };
                    if tx.send(msg).is_ok() {
                        count += 1;
                    }
                }
            }
        }

        // Pattern subscribers
        for (pattern, subs) in &inner.patterns {
            if glob_match(pattern, channel) {
                for &sub_id in subs {
                    if let Some(tx) = inner.senders.get(&sub_id) {
                        let msg = SubMessage {
                            channel: pattern.clone(),
                            actual_channel: channel.to_string(),
                            payload: message.to_string(),
                            is_pattern: true,
                        };
                        if tx.send(msg).is_ok() {
                            count += 1;
                        }
                    }
                }
            }
        }

        count
    }

    /// Remove a subscriber entirely (called on connection close).
    pub fn remove_subscriber(&self, sub_id: u64) {
        let mut inner = self.inner.lock();
        // Remove from all channels
        if let Some(channels) = inner.sub_channels.remove(&sub_id) {
            for ch in channels {
                if let Some(subs) = inner.channels.get_mut(&ch) {
                    subs.remove(&sub_id);
                    if subs.is_empty() {
                        inner.channels.remove(&ch);
                    }
                }
            }
        }
        // Remove from all patterns
        if let Some(patterns) = inner.sub_patterns.remove(&sub_id) {
            for pat in patterns {
                if let Some(subs) = inner.patterns.get_mut(&pat) {
                    subs.remove(&sub_id);
                    if subs.is_empty() {
                        inner.patterns.remove(&pat);
                    }
                }
            }
        }
        inner.senders.remove(&sub_id);
    }

    /// Get the total subscription count (channels + patterns) for a subscriber.
    pub fn subscription_count(&self, sub_id: u64) -> usize {
        let inner = self.inner.lock();
        let ch = inner.sub_channels.get(&sub_id).map_or(0, |s| s.len());
        let pat = inner.sub_patterns.get(&sub_id).map_or(0, |s| s.len());
        ch + pat
    }
}

/// Simple glob pattern matching (supports `*`, `?`, and `[...]`).
fn glob_match(pattern: &str, input: &str) -> bool {
    let pat: Vec<char> = pattern.chars().collect();
    let inp: Vec<char> = input.chars().collect();
    glob_match_inner(&pat, &inp, 0, 0)
}

fn glob_match_inner(pat: &[char], inp: &[char], pi: usize, ii: usize) -> bool {
    if pi == pat.len() {
        return ii == inp.len();
    }
    match pat[pi] {
        '*' => {
            // Try matching zero or more characters
            for skip in 0..=(inp.len() - ii) {
                if glob_match_inner(pat, inp, pi + 1, ii + skip) {
                    return true;
                }
            }
            false
        }
        '?' => {
            if ii < inp.len() {
                glob_match_inner(pat, inp, pi + 1, ii + 1)
            } else {
                false
            }
        }
        '[' => {
            if ii >= inp.len() {
                return false;
            }
            let mut pi2 = pi + 1;
            let negate = pi2 < pat.len() && pat[pi2] == '^';
            if negate {
                pi2 += 1;
            }
            let mut found = false;
            while pi2 < pat.len() && pat[pi2] != ']' {
                if pat[pi2] == inp[ii] {
                    found = true;
                }
                pi2 += 1;
            }
            if pi2 < pat.len() {
                pi2 += 1; // skip ']'
            }
            if found != negate {
                glob_match_inner(pat, inp, pi2, ii + 1)
            } else {
                false
            }
        }
        c => {
            if ii < inp.len() && inp[ii] == c {
                glob_match_inner(pat, inp, pi + 1, ii + 1)
            } else {
                false
            }
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_glob_match() {
        assert!(glob_match("foo", "foo"));
        assert!(!glob_match("foo", "bar"));
        assert!(glob_match("*", "anything"));
        assert!(glob_match("foo*", "foobar"));
        assert!(glob_match("f?o", "foo"));
        assert!(!glob_match("f?o", "fooo"));
        assert!(glob_match("channel.*", "channel.news"));
        assert!(glob_match("channel.*", "channel.sports"));
        assert!(!glob_match("channel.*", "other.news"));
        assert!(glob_match("h[ae]llo", "hello"));
        assert!(glob_match("h[ae]llo", "hallo"));
        assert!(!glob_match("h[ae]llo", "hillo"));
    }

    #[test]
    fn test_subscribe_publish() {
        let registry = PubSubRegistry::new();
        let (id1, mut sub1) = registry.new_subscriber();
        let (id2, mut sub2) = registry.new_subscriber();

        registry.subscribe(id1, "news");
        registry.subscribe(id2, "news");
        registry.subscribe(id2, "sports");

        let count = registry.publish("news", "hello");
        assert_eq!(count, 2);

        let msg1 = sub1.rx.try_recv().unwrap();
        assert_eq!(msg1.channel, "news");
        assert_eq!(msg1.payload, "hello");

        let msg2 = sub2.rx.try_recv().unwrap();
        assert_eq!(msg2.channel, "news");
        assert_eq!(msg2.payload, "hello");
    }

    #[test]
    fn test_pattern_subscribe() {
        let registry = PubSubRegistry::new();
        let (id1, mut sub1) = registry.new_subscriber();

        registry.psubscribe(id1, "news.*");

        let count = registry.publish("news.tech", "ai update");
        assert_eq!(count, 1);

        let msg = sub1.rx.try_recv().unwrap();
        assert_eq!(msg.actual_channel, "news.tech");
        assert_eq!(msg.payload, "ai update");
        assert!(msg.is_pattern);
    }

    #[test]
    fn test_unsubscribe() {
        let registry = PubSubRegistry::new();
        let (id1, _sub1) = registry.new_subscriber();

        assert_eq!(registry.subscribe(id1, "a"), 1);
        assert_eq!(registry.subscribe(id1, "b"), 2);
        assert_eq!(registry.unsubscribe(id1, "a"), 1);
        assert_eq!(registry.unsubscribe(id1, "b"), 0);
    }

    #[test]
    fn test_remove_subscriber() {
        let registry = PubSubRegistry::new();
        let (id1, _sub1) = registry.new_subscriber();
        registry.subscribe(id1, "ch");
        registry.psubscribe(id1, "pat.*");
        registry.remove_subscriber(id1);
        // Publishing should reach nobody
        assert_eq!(registry.publish("ch", "msg"), 0);
        assert_eq!(registry.publish("pat.x", "msg"), 0);
    }
}
