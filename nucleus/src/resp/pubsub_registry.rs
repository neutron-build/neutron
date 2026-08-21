//! Shared Pub/Sub registry for RESP connections.
//!
//! Wraps the Nucleus `PubSubHub` with thread-safe access and provides a
//! channel-based subscription model suitable for the async RESP server loop.
//! Each subscriber gets a `tokio::sync::mpsc` sender/receiver pair so the
//! connection loop can `select!` between incoming commands and outgoing
//! subscription messages.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;

/// Maximum number of undelivered messages queued for one subscriber.
///
/// The queue is bounded on two axes because either one alone is a hole: a
/// message cap alone lets 1 MB payloads reach gigabytes, and a byte cap alone
/// lets a flood of tiny messages cost more in per-message allocator overhead
/// than the payload bytes admit to.
pub const SUBSCRIBER_QUEUE_CAPACITY: usize = 1024;

/// Maximum number of undelivered payload bytes queued for one subscriber.
///
/// Matches Redis's default `client-output-buffer-limit pubsub` hard limit of
/// 32 MB. Redis kills the client at that point and so do we — see
/// [`PubSubRegistry::publish`].
pub const SUBSCRIBER_QUEUE_MAX_BYTES: usize = 32 * 1024 * 1024;

/// Longest `PSUBSCRIBE` pattern accepted. Matching is linear in the pattern
/// length (see [`glob_match`]), so this is a courtesy bound on per-publish work
/// and on the memory a subscriber can pin in the registry, not a correctness
/// requirement.
pub const MAX_PATTERN_LEN: usize = 1024;

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

impl SubMessage {
    /// Approximate heap cost of this message while it sits in a queue. Used
    /// for the per-subscriber output-buffer limit; it does not need to be
    /// exact, only monotone in the payload it charges for.
    fn queued_bytes(&self) -> usize {
        self.channel.len() + self.actual_channel.len() + self.payload.len()
    }
}

/// Per-subscriber handle returned when subscribing. The RESP server loop reads
/// from it to push messages to the client.
///
/// `rx` is deliberately private: every dequeue must go through [`Self::recv`]
/// or [`Self::try_recv`] so the queued-byte accounting stays in step with the
/// channel. A receiver that drained `rx` directly would leave `queued_bytes`
/// permanently high and get itself killed as a slow subscriber.
pub struct Subscription {
    rx: mpsc::Receiver<SubMessage>,
    queued_bytes: Arc<AtomicUsize>,
}

impl Subscription {
    /// Await the next message. `None` means the registry dropped this
    /// subscriber's sender — either the connection was cleaned up, or the
    /// subscriber exceeded its output-buffer limit and was killed.
    pub async fn recv(&mut self) -> Option<SubMessage> {
        let msg = self.rx.recv().await?;
        self.queued_bytes
            .fetch_sub(msg.queued_bytes(), Ordering::Relaxed);
        Some(msg)
    }

    /// Non-blocking dequeue, for tests and for draining.
    pub fn try_recv(&mut self) -> Result<SubMessage, mpsc::error::TryRecvError> {
        let msg = self.rx.try_recv()?;
        self.queued_bytes
            .fetch_sub(msg.queued_bytes(), Ordering::Relaxed);
        Ok(msg)
    }
}

/// The registry's end of one subscriber's queue, plus the byte accounting that
/// bounds it.
struct SubscriberSink {
    tx: mpsc::Sender<SubMessage>,
    queued_bytes: Arc<AtomicUsize>,
}

/// Shared pub/sub state across all RESP connections.
pub struct PubSubRegistry {
    inner: Mutex<PubSubInner>,
}

struct PubSubInner {
    /// channel → set of subscriber IDs
    channels: HashMap<String, HashSet<u64>>,
    /// subscriber_id → bounded sender + its queued-byte counter
    senders: HashMap<u64, SubscriberSink>,
    /// subscriber_id → set of subscribed channels
    sub_channels: HashMap<u64, HashSet<String>>,
    /// pattern → set of subscriber IDs
    patterns: HashMap<String, HashSet<u64>>,
    /// subscriber_id → set of subscribed patterns
    sub_patterns: HashMap<u64, HashSet<String>>,
    next_id: u64,
}

impl Default for PubSubRegistry {
    fn default() -> Self {
        Self::new()
    }
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
        let (tx, rx) = mpsc::channel(SUBSCRIBER_QUEUE_CAPACITY);
        let queued_bytes = Arc::new(AtomicUsize::new(0));
        let mut inner = self.inner.lock();
        let id = inner.next_id;
        inner.next_id += 1;
        inner.senders.insert(
            id,
            SubscriberSink {
                tx,
                queued_bytes: Arc::clone(&queued_bytes),
            },
        );
        inner.sub_channels.insert(id, HashSet::new());
        inner.sub_patterns.insert(id, HashSet::new());
        (id, Subscription { rx, queued_bytes })
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
    ///
    /// Each subscriber's queue is bounded by [`SUBSCRIBER_QUEUE_CAPACITY`]
    /// messages and [`SUBSCRIBER_QUEUE_MAX_BYTES`] bytes. A subscriber that
    /// hits either bound is **killed**, not skipped: its sender is dropped,
    /// which makes [`Subscription::recv`] return `None` once the already-queued
    /// messages have drained, and the RESP connection loop closes the
    /// connection. That is Redis's `client-output-buffer-limit pubsub`
    /// behaviour, and it is the right one here for the same reason: pub/sub is
    /// fire-and-forget, so silently dropping messages would leave the client
    /// believing it had a complete view of the channel when it had a
    /// gap it can neither see nor recover. Disconnecting is a signal the
    /// client cannot miss.
    pub fn publish(&self, channel: &str, message: &str) -> usize {
        let mut inner = self.inner.lock();
        let mut count = 0;
        // Subscribers over their output-buffer limit. Collected rather than
        // removed in place because the send loops hold a borrow of `inner`.
        let mut slow: Vec<u64> = Vec::new();

        // Direct subscribers
        if let Some(subs) = inner.channels.get(channel) {
            for &sub_id in subs {
                if let Some(sink) = inner.senders.get(&sub_id) {
                    let msg = SubMessage {
                        channel: channel.to_string(),
                        actual_channel: channel.to_string(),
                        payload: message.to_string(),
                        is_pattern: false,
                    };
                    match try_enqueue(sink, msg) {
                        EnqueueOutcome::Sent => count += 1,
                        EnqueueOutcome::Overflow => slow.push(sub_id),
                        EnqueueOutcome::Closed => {}
                    }
                }
            }
        }

        // Pattern subscribers
        for (pattern, subs) in &inner.patterns {
            if glob_match(pattern, channel) {
                for &sub_id in subs {
                    if let Some(sink) = inner.senders.get(&sub_id) {
                        let msg = SubMessage {
                            channel: pattern.clone(),
                            actual_channel: channel.to_string(),
                            payload: message.to_string(),
                            is_pattern: true,
                        };
                        match try_enqueue(sink, msg) {
                            EnqueueOutcome::Sent => count += 1,
                            EnqueueOutcome::Overflow => slow.push(sub_id),
                            EnqueueOutcome::Closed => {}
                        }
                    }
                }
            }
        }

        for sub_id in slow {
            // Dropping the sender is the kill. The subscription sets are left
            // in place so the connection stays in pub/sub mode and reaches the
            // `None` from `recv` that closes it.
            if inner.senders.remove(&sub_id).is_some() {
                tracing::warn!(
                    subscriber = sub_id,
                    channel = channel,
                    capacity = SUBSCRIBER_QUEUE_CAPACITY,
                    max_bytes = SUBSCRIBER_QUEUE_MAX_BYTES,
                    "RESP pub/sub subscriber exceeded its output buffer limit; disconnecting"
                );
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

/// What happened when a message was offered to one subscriber's queue.
enum EnqueueOutcome {
    Sent,
    /// The queue is at its message or byte limit. The subscriber must be
    /// killed; see [`PubSubRegistry::publish`].
    Overflow,
    /// The receiver is already gone.
    Closed,
}

/// Offer a message to one subscriber without blocking, charging its size to
/// the subscriber's output-buffer accounting.
fn try_enqueue(sink: &SubscriberSink, msg: SubMessage) -> EnqueueOutcome {
    let size = msg.queued_bytes();
    // Check the byte limit before the message limit: a handful of very large
    // payloads must not be able to sit under a 1024-message cap.
    if sink
        .queued_bytes
        .load(Ordering::Relaxed)
        .saturating_add(size)
        > SUBSCRIBER_QUEUE_MAX_BYTES
    {
        return EnqueueOutcome::Overflow;
    }
    match sink.tx.try_send(msg) {
        Ok(()) => {
            sink.queued_bytes.fetch_add(size, Ordering::Relaxed);
            EnqueueOutcome::Sent
        }
        Err(TrySendError::Full(_)) => EnqueueOutcome::Overflow,
        Err(TrySendError::Closed(_)) => EnqueueOutcome::Closed,
    }
}

/// Glob pattern matching (supports `*`, `?`, and `[...]`).
///
/// Iterative two-pointer, with a single backtrack point per `*`. Cost is
/// O(pattern × input) in the worst case and O(pattern + input) in the common
/// one, with no recursion and no allocation beyond the two `char` vectors.
///
/// The previous implementation recursed once per pattern character and tried
/// every split point at every `*`, so a client-supplied `PSUBSCRIBE
/// 'a*a*a*a*…b'` cost exponential time — under the registry's global mutex,
/// stalling every other subscriber — and a long pattern drove stack depth
/// linearly toward an overflow, which aborts the process (S31-09).
///
/// Patterns longer than [`MAX_PATTERN_LEN`] never match. `PSUBSCRIBE` rejects
/// them up front; this is the backstop for any other caller.
fn glob_match(pattern: &str, input: &str) -> bool {
    if pattern.len() > MAX_PATTERN_LEN {
        return false;
    }
    let pat: Vec<char> = pattern.chars().collect();
    let inp: Vec<char> = input.chars().collect();

    let mut pi = 0usize;
    let mut ii = 0usize;
    // Where to resume if the current `*` turns out to have consumed too little.
    let mut star_pi: Option<usize> = None;
    let mut star_ii = 0usize;

    while ii < inp.len() {
        if pi < pat.len() && pat[pi] == '*' {
            star_pi = Some(pi);
            star_ii = ii;
            pi += 1;
        } else if let Some(next_pi) = match_one(&pat, pi, inp[ii]) {
            pi = next_pi;
            ii += 1;
        } else if let Some(spi) = star_pi {
            // The last `*` must swallow one more input character.
            pi = spi + 1;
            star_ii += 1;
            ii = star_ii;
        } else {
            return false;
        }
    }

    // Trailing `*`s can match the empty remainder; nothing else can.
    while pi < pat.len() && pat[pi] == '*' {
        pi += 1;
    }
    pi == pat.len()
}

/// Match the single pattern unit starting at `pi` against `ch`.
///
/// Returns the index just past the unit on a match, `None` otherwise. A unit is
/// one of `?`, a `[...]` class, or a literal character — never `*`, which the
/// caller handles.
fn match_one(pat: &[char], pi: usize, ch: char) -> Option<usize> {
    if pi >= pat.len() {
        return None;
    }
    match pat[pi] {
        '*' => None,
        '?' => Some(pi + 1),
        '[' => {
            let mut p = pi + 1;
            let negate = p < pat.len() && pat[p] == '^';
            if negate {
                p += 1;
            }
            let mut found = false;
            while p < pat.len() && pat[p] != ']' {
                if pat[p] == ch {
                    found = true;
                }
                p += 1;
            }
            if p < pat.len() {
                p += 1; // skip ']'
            }
            // An unterminated class consumes the rest of the pattern, matching
            // the pre-existing behaviour.
            if found != negate { Some(p) } else { None }
        }
        c => {
            if c == ch {
                Some(pi + 1)
            } else {
                None
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

        let msg1 = sub1.try_recv().unwrap();
        assert_eq!(msg1.channel, "news");
        assert_eq!(msg1.payload, "hello");

        let msg2 = sub2.try_recv().unwrap();
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

        let msg = sub1.try_recv().unwrap();
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

    // ── S31-08: bounded subscriber queues ────────────────────────────────

    /// A subscriber that never reads must not be able to grow the server heap
    /// without bound. Before the fix the channel was `unbounded_channel()` and
    /// this loop enqueued a million `SubMessage`s, each holding a full copy of
    /// the payload.
    #[test]
    fn slow_subscriber_queue_is_bounded_and_the_subscriber_is_killed() {
        let registry = PubSubRegistry::new();
        let (id, mut sub) = registry.new_subscriber();
        registry.subscribe(id, "flood");

        // Publish far past the queue capacity without ever reading.
        let mut delivered = 0usize;
        for _ in 0..(SUBSCRIBER_QUEUE_CAPACITY * 4) {
            delivered += registry.publish("flood", "payload");
        }

        // At most one full queue was ever accepted.
        assert!(
            delivered <= SUBSCRIBER_QUEUE_CAPACITY,
            "queue accepted {delivered} messages, capacity is {SUBSCRIBER_QUEUE_CAPACITY}"
        );

        // The subscriber was killed, not merely throttled: publishes now reach
        // nobody, and the receiver sees the close once it has drained.
        assert_eq!(registry.publish("flood", "again"), 0);
        let mut drained = 0usize;
        while sub.try_recv().is_ok() {
            drained += 1;
        }
        assert_eq!(
            drained, delivered,
            "every accepted message is still readable"
        );
        assert!(
            matches!(sub.try_recv(), Err(mpsc::error::TryRecvError::Disconnected)),
            "a killed subscriber's channel must close so the connection loop drops it"
        );
    }

    /// The byte limit has to bite before the message limit when payloads are
    /// large — 1024 × 1 MB is a gigabyte, which is not a bound worth having.
    #[test]
    fn large_payloads_hit_the_byte_limit_before_the_message_limit() {
        let registry = PubSubRegistry::new();
        let (id, _sub) = registry.new_subscriber();
        registry.subscribe(id, "big");

        let payload = "x".repeat(1024 * 1024); // 1 MiB
        let mut delivered = 0usize;
        for _ in 0..SUBSCRIBER_QUEUE_CAPACITY {
            delivered += registry.publish("big", &payload);
        }
        assert!(
            delivered <= SUBSCRIBER_QUEUE_MAX_BYTES / payload.len() + 1,
            "queued {delivered} MiB, limit is {} MiB",
            SUBSCRIBER_QUEUE_MAX_BYTES / (1024 * 1024)
        );
        assert!(delivered > 0, "the limit must not reject the first message");
    }

    /// Draining returns capacity: a subscriber that keeps up is never killed.
    #[test]
    fn a_subscriber_that_reads_keeps_receiving() {
        let registry = PubSubRegistry::new();
        let (id, mut sub) = registry.new_subscriber();
        registry.subscribe(id, "ok");
        for i in 0..(SUBSCRIBER_QUEUE_CAPACITY * 3) {
            assert_eq!(registry.publish("ok", &format!("m{i}")), 1, "message {i}");
            let msg = sub.try_recv().expect("message is available");
            assert_eq!(msg.payload, format!("m{i}"));
        }
    }

    // ── S31-09: linear glob matching ─────────────────────────────────────

    /// The classic pathological glob input. Against the old recursive matcher
    /// this is exponential in the number of `*`; 20 stars against 64
    /// non-matching characters does not terminate in any useful time. It runs
    /// under the registry's global mutex, so it stalls every other subscriber.
    #[test]
    fn pathological_glob_pattern_completes_immediately() {
        let pattern = format!("{}b", "a*".repeat(20));
        let input = "a".repeat(64);
        let start = std::time::Instant::now();
        assert!(!glob_match(&pattern, &input));
        let elapsed = start.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(100),
            "pathological glob took {elapsed:?}"
        );
    }

    /// Same shape, reached the way a remote client reaches it: PSUBSCRIBE then
    /// PUBLISH, with the match running inside `publish` under the global mutex.
    #[test]
    fn pathological_pattern_does_not_stall_publish() {
        let registry = PubSubRegistry::new();
        let (id, _sub) = registry.new_subscriber();
        registry.psubscribe(id, &format!("{}b", "a*".repeat(20)));
        let channel = "a".repeat(64);
        let start = std::time::Instant::now();
        assert_eq!(registry.publish(&channel, "x"), 0);
        let elapsed = start.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(100),
            "publish against a pathological pattern took {elapsed:?}"
        );
    }

    /// A long pattern drove recursion depth linearly in the old matcher, and a
    /// stack overflow aborts the process rather than unwinding. The iterative
    /// matcher has no stack cost at all; this would have been ~200k frames.
    #[test]
    fn very_long_patterns_do_not_recurse() {
        let pattern = "?".repeat(MAX_PATTERN_LEN);
        assert!(glob_match(&pattern, &"z".repeat(MAX_PATTERN_LEN)));
        // Past the cap, nothing matches — PSUBSCRIBE rejects these up front.
        let over = "?".repeat(MAX_PATTERN_LEN + 1);
        assert!(!glob_match(&over, &"z".repeat(MAX_PATTERN_LEN + 1)));
    }

    /// The iterative matcher must agree with the old recursive one on every
    /// case the old one got right.
    #[test]
    fn glob_semantics_are_unchanged() {
        assert!(glob_match("", ""));
        assert!(!glob_match("", "a"));
        assert!(glob_match("*", ""));
        assert!(glob_match("**", "abc"));
        assert!(glob_match("a*b*c", "axxbyyc"));
        assert!(!glob_match("a*b*c", "axxbyy"));
        assert!(glob_match("*.log", "app.log"));
        assert!(!glob_match("*.log", "app.txt"));
        assert!(glob_match("a?c*", "abcdef"));
        assert!(glob_match("[a-c]", "-")); // ranges are literal sets, as before
        assert!(glob_match("h[^i]llo", "hello"));
        assert!(!glob_match("h[^e]llo", "hello"));
        assert!(glob_match("news.*.tech", "news.uk.tech"));
        assert!(!glob_match("news.*.tech", "news.uk.sport"));
        assert!(glob_match("*a", "aaa"));
        assert!(glob_match("a*", "a"));
        assert!(!glob_match("a*", "b"));
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
