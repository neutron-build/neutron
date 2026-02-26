//! Pub/Sub messaging and job queue system.
//!
//! Supports:
//!   - Publish/subscribe channels (fan-out messaging)
//!   - Job queues with priorities, retries, and dead-letter
//!   - Reliable delivery with acknowledgments
//!
//! Replaces Redis Pub/Sub, BullMQ, Redis Streams.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::broadcast;
use std::collections::HashSet;

// ============================================================================
// Pub/Sub
// ============================================================================

/// A message published to a channel.
#[derive(Debug, Clone)]
pub struct Message {
    pub channel: String,
    pub payload: String,
    pub timestamp: u64,
}

/// Pub/Sub hub — manages channels and subscriptions.
pub struct PubSubHub {
    /// channel → broadcast sender.
    channels: HashMap<String, broadcast::Sender<Arc<Message>>>,
    /// Default channel capacity.
    capacity: usize,
}

impl PubSubHub {
    pub fn new(capacity: usize) -> Self {
        Self {
            channels: HashMap::new(),
            capacity,
        }
    }

    /// Publish a message to a channel. Returns number of active subscribers.
    pub fn publish(&mut self, channel: &str, payload: String) -> usize {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let msg = Arc::new(Message {
            channel: channel.to_string(),
            payload,
            timestamp: ts,
        });

        if let Some(tx) = self.channels.get(channel) {
            tx.send(msg).unwrap_or(0)
        } else {
            0
        }
    }

    /// Subscribe to a channel. Returns a receiver for messages.
    pub fn subscribe(&mut self, channel: &str) -> broadcast::Receiver<Arc<Message>> {
        let tx = self
            .channels
            .entry(channel.to_string())
            .or_insert_with(|| broadcast::channel(self.capacity).0);
        tx.subscribe()
    }

    /// Get the number of subscribers on a channel.
    pub fn subscriber_count(&self, channel: &str) -> usize {
        self.channels
            .get(channel)
            .map(|tx| tx.receiver_count())
            .unwrap_or(0)
    }

    /// List all active channels.
    pub fn channels(&self) -> Vec<&str> {
        self.channels.keys().map(|s| s.as_str()).collect()
    }
}

// ============================================================================
// Job Queue
// ============================================================================

/// Priority level for jobs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Priority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}

/// A job in the queue.
#[derive(Debug, Clone)]
pub struct Job {
    pub id: u64,
    pub queue: String,
    pub payload: String,
    pub priority: Priority,
    pub max_retries: u32,
    pub retry_count: u32,
    pub status: JobStatus,
    pub created_at: u64,
}

/// Job status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobStatus {
    Pending,
    Processing,
    Completed,
    Failed,
    DeadLetter,
}

/// Job queue with priorities, retries, and dead-letter.
pub struct JobQueue {
    /// Queue name → priority queue of pending jobs (BTreeMap key = (priority_inv, id) for ordering).
    queues: HashMap<String, BTreeMap<(u8, u64), Job>>,
    /// All jobs by ID.
    jobs: HashMap<u64, Job>,
    /// Dead-letter queue per queue name.
    dead_letter: HashMap<String, VecDeque<Job>>,
    /// Next job ID.
    next_id: u64,
}

impl JobQueue {
    pub fn new() -> Self {
        Self {
            queues: HashMap::new(),
            jobs: HashMap::new(),
            dead_letter: HashMap::new(),
            next_id: 1,
        }
    }

    /// Add a job to a queue. Returns the job ID.
    pub fn enqueue(
        &mut self,
        queue: &str,
        payload: String,
        priority: Priority,
        max_retries: u32,
    ) -> u64 {
        let id = self.next_id;
        self.next_id += 1;

        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let job = Job {
            id,
            queue: queue.to_string(),
            payload,
            priority,
            max_retries,
            retry_count: 0,
            status: JobStatus::Pending,
            created_at: ts,
        };

        // Priority key: invert priority so Critical (3) sorts first (lower key = first in BTreeMap)
        let key = (3 - priority as u8, id);
        self.queues
            .entry(queue.to_string())
            .or_default()
            .insert(key, job.clone());
        self.jobs.insert(id, job);

        id
    }

    /// Dequeue the highest-priority pending job. Returns it in Processing state.
    pub fn dequeue(&mut self, queue: &str) -> Option<Job> {
        let btree = self.queues.get_mut(queue)?;
        let key = *btree.keys().next()?;
        let mut job = btree.remove(&key)?;
        job.status = JobStatus::Processing;
        self.jobs.insert(job.id, job.clone());
        Some(job)
    }

    /// Mark a job as completed.
    pub fn complete(&mut self, job_id: u64) -> bool {
        if let Some(job) = self.jobs.get_mut(&job_id) {
            job.status = JobStatus::Completed;
            true
        } else {
            false
        }
    }

    /// Mark a job as failed. Retries if under max_retries, otherwise moves to dead-letter.
    pub fn fail(&mut self, job_id: u64) -> JobStatus {
        let job = match self.jobs.get_mut(&job_id) {
            Some(j) => j,
            None => return JobStatus::Failed,
        };

        job.retry_count += 1;
        if job.retry_count <= job.max_retries {
            // Re-enqueue
            job.status = JobStatus::Pending;
            let key = (3 - job.priority as u8, job.id);
            self.queues
                .entry(job.queue.clone())
                .or_default()
                .insert(key, job.clone());
            JobStatus::Pending
        } else {
            // Dead letter
            job.status = JobStatus::DeadLetter;
            self.dead_letter
                .entry(job.queue.clone())
                .or_default()
                .push_back(job.clone());
            JobStatus::DeadLetter
        }
    }

    /// Get the number of pending jobs in a queue.
    pub fn pending_count(&self, queue: &str) -> usize {
        self.queues.get(queue).map_or(0, |q| q.len())
    }

    /// Get dead-letter queue contents.
    pub fn dead_letter_jobs(&self, queue: &str) -> Vec<&Job> {
        self.dead_letter
            .get(queue)
            .map(|q| q.iter().collect())
            .unwrap_or_default()
    }

    /// Get a job by ID.
    pub fn get_job(&self, job_id: u64) -> Option<&Job> {
        self.jobs.get(&job_id)
    }
}

// ============================================================================
// Distributed Pub/Sub Router
// ============================================================================

/// Routes pub/sub messages across cluster nodes via gossip-based subscription
/// propagation. NOTIFY on one node delivers to LISTEN on all nodes.
pub struct DistributedPubSubRouter {
    /// node_id → set of channels that node has subscribers for.
    node_subscriptions: HashMap<u64, std::collections::HashSet<String>>,
    /// Local node's hub.
    local_hub: PubSubHub,
    /// Local node ID.
    local_node_id: u64,
    /// Messages forwarded to remote nodes (buffered for delivery).
    outbox: Vec<RemotePubSubMessage>,
}

/// A message that needs to be forwarded to a remote node.
#[derive(Debug, Clone)]
pub struct RemotePubSubMessage {
    pub target_node: u64,
    pub channel: String,
    pub payload: String,
}

impl DistributedPubSubRouter {
    pub fn new(local_node_id: u64, hub_capacity: usize) -> Self {
        Self {
            node_subscriptions: HashMap::new(),
            local_hub: PubSubHub::new(hub_capacity),
            local_node_id,
            outbox: Vec::new(),
        }
    }

    /// Register that a remote node has subscribers on a channel.
    pub fn register_remote_subscription(&mut self, node_id: u64, channel: &str) {
        self.node_subscriptions
            .entry(node_id)
            .or_default()
            .insert(channel.to_string());
    }

    /// Unregister a remote node's subscription.
    pub fn unregister_remote_subscription(&mut self, node_id: u64, channel: &str) {
        if let Some(channels) = self.node_subscriptions.get_mut(&node_id) {
            channels.remove(channel);
        }
    }

    /// Subscribe locally and propagate subscription info to other nodes.
    pub fn subscribe_local(
        &mut self,
        channel: &str,
    ) -> broadcast::Receiver<Arc<Message>> {
        self.local_hub.subscribe(channel)
    }

    /// Publish to local hub AND generate forwarding messages for remote nodes
    /// that have subscribers on this channel.
    pub fn publish(&mut self, channel: &str, payload: String) -> (usize, usize) {
        let local_count = self.local_hub.publish(channel, payload.clone());

        let mut remote_count = 0;
        for (&node_id, channels) in &self.node_subscriptions {
            if node_id == self.local_node_id {
                continue;
            }
            if channels.contains(channel) {
                self.outbox.push(RemotePubSubMessage {
                    target_node: node_id,
                    channel: channel.to_string(),
                    payload: payload.clone(),
                });
                remote_count += 1;
            }
        }

        (local_count, remote_count)
    }

    /// Receive a message from a remote node and deliver to local subscribers.
    pub fn deliver_remote(&mut self, channel: &str, payload: String) -> usize {
        self.local_hub.publish(channel, payload)
    }

    /// Drain the outbox of messages to send to remote nodes.
    pub fn drain_outbox(&mut self) -> Vec<RemotePubSubMessage> {
        std::mem::take(&mut self.outbox)
    }

    /// Get the gossip snapshot: which channels this node subscribes to.
    pub fn local_subscription_snapshot(&self) -> Vec<String> {
        self.local_hub
            .channels()
            .into_iter()
            .filter(|ch| self.local_hub.subscriber_count(ch) > 0)
            .map(|s| s.to_string())
            .collect()
    }

    /// Apply a gossip message: update our knowledge of a remote node's subscriptions.
    pub fn apply_gossip(&mut self, node_id: u64, channels: Vec<String>) {
        let set: std::collections::HashSet<String> = channels.into_iter().collect();
        self.node_subscriptions.insert(node_id, set);
    }

    /// Number of remote nodes tracked.
    pub fn remote_node_count(&self) -> usize {
        self.node_subscriptions
            .keys()
            .filter(|&&nid| nid != self.local_node_id)
            .count()
    }

    /// Outbox length.
    pub fn outbox_len(&self) -> usize {
        self.outbox.len()
    }
}


// =========================================================================
// Streams (Redis-style with Consumer Groups)
// =========================================================================

/// Unique identifier for a stream entry.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamEntryId {
    pub ms: u64,
    pub seq: u64,
}

impl StreamEntryId {
    pub fn new(ms: u64, seq: u64) -> Self {
        Self { ms, seq }
    }
}

impl PartialOrd for StreamEntryId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for StreamEntryId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.ms.cmp(&other.ms).then(self.seq.cmp(&other.seq))
    }
}

impl std::fmt::Display for StreamEntryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

/// A single entry in a stream.
#[derive(Debug, Clone)]
pub struct StreamEntry {
    pub id: StreamEntryId,
    pub fields: Vec<(String, String)>,
}

/// A consumer group tracks delivery and pending acknowledgments.
#[derive(Debug)]
pub struct ConsumerGroup {
    pub name: String,
    pub last_delivered_id: StreamEntryId,
    pub pending: HashMap<String, Vec<StreamEntryId>>,
    pub consumers: HashSet<String>,
}

/// An append-only stream with consumer groups and optional max length.
pub struct Stream {
    pub entries: Vec<StreamEntry>,
    pub last_id: StreamEntryId,
    pub groups: HashMap<String, ConsumerGroup>,
    pub max_len: Option<usize>,
}

impl Stream {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            last_id: StreamEntryId::new(0, 0),
            groups: HashMap::new(),
            max_len: None,
        }
    }

    pub fn with_max_len(max_len: usize) -> Self {
        Self {
            entries: Vec::new(),
            last_id: StreamEntryId::new(0, 0),
            groups: HashMap::new(),
            max_len: Some(max_len),
        }
    }

    fn next_id(&self) -> StreamEntryId {
        let ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        if ms > self.last_id.ms {
            StreamEntryId::new(ms, 0)
        } else {
            StreamEntryId::new(self.last_id.ms, self.last_id.seq + 1)
        }
    }

    pub fn xadd(&mut self, fields: Vec<(String, String)>) -> StreamEntryId {
        let id = self.next_id();
        self.xadd_with_id(id, fields)
    }

    pub fn xadd_with_id(&mut self, id: StreamEntryId, fields: Vec<(String, String)>) -> StreamEntryId {
        self.last_id = id.clone();
        self.entries.push(StreamEntry { id: id.clone(), fields });
        if let Some(max) = self.max_len {
            if self.entries.len() > max {
                let excess = self.entries.len() - max;
                self.entries.drain(..excess);
            }
        }
        id
    }

    pub fn xlen(&self) -> usize {
        self.entries.len()
    }

    pub fn xrange(&self, start: &StreamEntryId, end: &StreamEntryId, count: Option<usize>) -> Vec<&StreamEntry> {
        let mut result = Vec::new();
        for entry in &self.entries {
            if entry.id >= *start && entry.id <= *end {
                result.push(entry);
                if let Some(c) = count {
                    if result.len() >= c { break; }
                }
            }
        }
        result
    }

    pub fn xread(&self, last_id: &StreamEntryId, count: usize) -> Vec<&StreamEntry> {
        let mut result = Vec::new();
        for entry in &self.entries {
            if entry.id > *last_id {
                result.push(entry);
                if result.len() >= count { break; }
            }
        }
        result
    }

    pub fn xgroup_create(&mut self, group_name: &str, start_id: StreamEntryId) {
        self.groups.insert(group_name.to_string(), ConsumerGroup {
            name: group_name.to_string(),
            last_delivered_id: start_id,
            pending: HashMap::new(),
            consumers: HashSet::new(),
        });
    }

    pub fn xreadgroup(&mut self, group: &str, consumer: &str, count: usize) -> Vec<&StreamEntry> {
        let last_delivered = match self.groups.get(group) {
            Some(g) => g.last_delivered_id.clone(),
            None => return Vec::new(),
        };
        let mut indices = Vec::new();
        let mut new_last = last_delivered.clone();
        for (i, entry) in self.entries.iter().enumerate() {
            if entry.id > last_delivered {
                indices.push(i);
                new_last = entry.id.clone();
                if indices.len() >= count { break; }
            }
        }
        if let Some(g) = self.groups.get_mut(group) {
            g.last_delivered_id = new_last;
            g.consumers.insert(consumer.to_string());
            let pending_ids: Vec<StreamEntryId> = indices.iter().map(|&i| self.entries[i].id.clone()).collect();
            g.pending.entry(consumer.to_string()).or_default().extend(pending_ids);
        }
        indices.iter().map(|&i| &self.entries[i]).collect()
    }

    pub fn xack(&mut self, group: &str, ids: &[StreamEntryId]) -> usize {
        let g = match self.groups.get_mut(group) {
            Some(g) => g,
            None => return 0,
        };
        let mut acked = 0;
        for consumer_pending in g.pending.values_mut() {
            let before = consumer_pending.len();
            consumer_pending.retain(|id| !ids.contains(id));
            acked += before - consumer_pending.len();
        }
        acked
    }

    pub fn xpending(&self, group: &str) -> Vec<(String, usize)> {
        match self.groups.get(group) {
            Some(g) => {
                let mut result: Vec<(String, usize)> = g.pending.iter()
                    .filter(|(_, ids)| !ids.is_empty())
                    .map(|(consumer, ids)| (consumer.clone(), ids.len()))
                    .collect();
                result.sort_by(|a, b| a.0.cmp(&b.0));
                result
            }
            None => Vec::new(),
        }
    }

    pub fn xtrim(&mut self, max_len: usize) -> usize {
        if self.entries.len() > max_len {
            let excess = self.entries.len() - max_len;
            self.entries.drain(..excess);
            excess
        } else {
            0
        }
    }

    pub fn xinfo_groups(&self) -> Vec<(&str, usize, usize)> {
        self.groups.values().map(|g| {
            let total_pending: usize = g.pending.values().map(|v| v.len()).sum();
            (g.name.as_str(), total_pending, g.consumers.len())
        }).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn pubsub_basic() {
        let mut hub = PubSubHub::new(16);
        let mut rx = hub.subscribe("events");

        hub.publish("events", "hello".to_string());

        let msg = rx.recv().await.unwrap();
        assert_eq!(msg.payload, "hello");
        assert_eq!(msg.channel, "events");
    }

    #[tokio::test]
    async fn pubsub_fanout() {
        let mut hub = PubSubHub::new(16);
        let mut rx1 = hub.subscribe("news");
        let mut rx2 = hub.subscribe("news");

        assert_eq!(hub.subscriber_count("news"), 2);

        let count = hub.publish("news", "breaking".to_string());
        assert_eq!(count, 2);

        let m1 = rx1.recv().await.unwrap();
        let m2 = rx2.recv().await.unwrap();
        assert_eq!(m1.payload, "breaking");
        assert_eq!(m2.payload, "breaking");
    }

    #[test]
    fn job_queue_priority() {
        let mut q = JobQueue::new();
        q.enqueue("tasks", "low-job".into(), Priority::Low, 0);
        q.enqueue("tasks", "critical-job".into(), Priority::Critical, 0);
        q.enqueue("tasks", "normal-job".into(), Priority::Normal, 0);

        // Should dequeue in priority order: Critical > Normal > Low
        let j1 = q.dequeue("tasks").unwrap();
        assert_eq!(j1.payload, "critical-job");
        let j2 = q.dequeue("tasks").unwrap();
        assert_eq!(j2.payload, "normal-job");
        let j3 = q.dequeue("tasks").unwrap();
        assert_eq!(j3.payload, "low-job");
    }

    #[test]
    fn job_retry_and_dead_letter() {
        let mut q = JobQueue::new();
        let id = q.enqueue("work", "flaky-job".into(), Priority::Normal, 2);

        let job = q.dequeue("work").unwrap();
        assert_eq!(job.status, JobStatus::Processing);

        // Fail it 3 times (max_retries = 2, so 3rd failure → dead letter)
        assert_eq!(q.fail(id), JobStatus::Pending); // retry 1
        let _ = q.dequeue("work");
        assert_eq!(q.fail(id), JobStatus::Pending); // retry 2
        let _ = q.dequeue("work");
        assert_eq!(q.fail(id), JobStatus::DeadLetter); // exceeded

        assert_eq!(q.dead_letter_jobs("work").len(), 1);
        assert_eq!(q.pending_count("work"), 0);
    }

    #[test]
    fn job_complete() {
        let mut q = JobQueue::new();
        let id = q.enqueue("tasks", "my-job".into(), Priority::High, 0);
        let _ = q.dequeue("tasks");
        assert!(q.complete(id));
        assert_eq!(q.get_job(id).unwrap().status, JobStatus::Completed);
    }

    // ====================================================================
    // Additional pubsub tests
    // ====================================================================

    #[tokio::test]
    async fn pubsub_multiple_subscribers_same_channel() {
        let mut hub = PubSubHub::new(16);
        let mut rx1 = hub.subscribe("alerts");
        let mut rx2 = hub.subscribe("alerts");
        let mut rx3 = hub.subscribe("alerts");

        assert_eq!(hub.subscriber_count("alerts"), 3);

        let count = hub.publish("alerts", "fire".to_string());
        assert_eq!(count, 3);

        let m1 = rx1.recv().await.unwrap();
        let m2 = rx2.recv().await.unwrap();
        let m3 = rx3.recv().await.unwrap();
        assert_eq!(m1.payload, "fire");
        assert_eq!(m2.payload, "fire");
        assert_eq!(m3.payload, "fire");
        // All receivers get the same Arc (pointer equality)
        assert!(Arc::ptr_eq(&m1, &m2));
        assert!(Arc::ptr_eq(&m2, &m3));
    }

    #[tokio::test]
    async fn pubsub_unsubscribe_by_dropping_receiver() {
        let mut hub = PubSubHub::new(16);
        let rx1 = hub.subscribe("ch");
        let _rx2 = hub.subscribe("ch");
        assert_eq!(hub.subscriber_count("ch"), 2);

        // Drop one receiver
        drop(rx1);
        assert_eq!(hub.subscriber_count("ch"), 1);

        // Publish should only reach 1 subscriber
        let count = hub.publish("ch", "msg".to_string());
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn pubsub_message_ordering() {
        let mut hub = PubSubHub::new(16);
        let mut rx = hub.subscribe("ordered");

        for i in 0..5 {
            hub.publish("ordered", format!("msg-{}", i));
        }

        // Messages must arrive in publication order
        for i in 0..5 {
            let msg = rx.recv().await.unwrap();
            assert_eq!(msg.payload, format!("msg-{}", i));
        }
    }

    #[test]
    fn pubsub_publish_to_nonexistent_channel() {
        let mut hub = PubSubHub::new(16);
        // No subscribers, publish returns 0
        let count = hub.publish("ghost", "hello".to_string());
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn pubsub_empty_channel_name_and_payload() {
        let mut hub = PubSubHub::new(16);
        let mut rx = hub.subscribe("");

        let count = hub.publish("", "".to_string());
        assert_eq!(count, 1);

        let msg = rx.recv().await.unwrap();
        assert_eq!(msg.channel, "");
        assert_eq!(msg.payload, "");
    }

    #[tokio::test]
    async fn pubsub_subscribe_unsubscribe_resubscribe() {
        let mut hub = PubSubHub::new(16);

        // Subscribe
        let rx1 = hub.subscribe("toggle");
        assert_eq!(hub.subscriber_count("toggle"), 1);

        // Unsubscribe by dropping
        drop(rx1);
        assert_eq!(hub.subscriber_count("toggle"), 0);

        // Resubscribe
        let mut rx2 = hub.subscribe("toggle");
        assert_eq!(hub.subscriber_count("toggle"), 1);

        // Should receive new messages after resubscribe
        hub.publish("toggle", "back".to_string());
        let msg = rx2.recv().await.unwrap();
        assert_eq!(msg.payload, "back");
    }

    #[test]
    fn pubsub_channels_listing() {
        let mut hub = PubSubHub::new(16);
        assert!(hub.channels().is_empty());

        let _rx1 = hub.subscribe("alpha");
        let _rx2 = hub.subscribe("beta");
        let _rx3 = hub.subscribe("gamma");

        let mut chans = hub.channels();
        chans.sort();
        assert_eq!(chans, vec!["alpha", "beta", "gamma"]);
    }

    #[tokio::test]
    async fn pubsub_multiple_channels_isolation() {
        let mut hub = PubSubHub::new(16);
        let mut rx_a = hub.subscribe("chan-a");
        let mut rx_b = hub.subscribe("chan-b");

        hub.publish("chan-a", "only-a".to_string());
        hub.publish("chan-b", "only-b".to_string());

        let msg_a = rx_a.recv().await.unwrap();
        let msg_b = rx_b.recv().await.unwrap();
        assert_eq!(msg_a.payload, "only-a");
        assert_eq!(msg_a.channel, "chan-a");
        assert_eq!(msg_b.payload, "only-b");
        assert_eq!(msg_b.channel, "chan-b");
    }

    // -- Distributed pub/sub router tests --

    #[tokio::test]
    async fn distributed_pubsub_local_delivery() {
        let mut router = DistributedPubSubRouter::new(1, 16);
        let mut rx = router.subscribe_local("events");
        let (local, remote) = router.publish("events", "hello".to_string());
        assert_eq!(local, 1);
        assert_eq!(remote, 0);
        let msg = rx.recv().await.unwrap();
        assert_eq!(msg.payload, "hello");
    }

    #[test]
    fn distributed_pubsub_remote_forwarding() {
        let mut router = DistributedPubSubRouter::new(1, 16);
        router.register_remote_subscription(2, "events");
        router.register_remote_subscription(3, "events");

        let (_, remote) = router.publish("events", "data".to_string());
        assert_eq!(remote, 2);
        assert_eq!(router.outbox_len(), 2);

        let outbox = router.drain_outbox();
        assert_eq!(outbox.len(), 2);
        assert!(outbox.iter().any(|m| m.target_node == 2));
        assert!(outbox.iter().any(|m| m.target_node == 3));
        assert_eq!(router.outbox_len(), 0);
    }

    #[tokio::test]
    async fn distributed_pubsub_deliver_remote() {
        let mut router = DistributedPubSubRouter::new(1, 16);
        let mut rx = router.subscribe_local("alerts");
        let count = router.deliver_remote("alerts", "remote-msg".to_string());
        assert_eq!(count, 1);
        let msg = rx.recv().await.unwrap();
        assert_eq!(msg.payload, "remote-msg");
    }

    #[test]
    fn distributed_pubsub_gossip_apply() {
        let mut router = DistributedPubSubRouter::new(1, 16);
        router.apply_gossip(2, vec!["a".into(), "b".into()]);
        router.apply_gossip(3, vec!["b".into(), "c".into()]);
        assert_eq!(router.remote_node_count(), 2);

        // Publishing to "b" should forward to both node 2 and 3.
        let (_, remote) = router.publish("b", "msg".to_string());
        assert_eq!(remote, 2);

        // Publishing to "a" should forward only to node 2.
        let _ = router.drain_outbox();
        let (_, remote) = router.publish("a", "msg".to_string());
        assert_eq!(remote, 1);
    }

    #[test]
    fn distributed_pubsub_unregister() {
        let mut router = DistributedPubSubRouter::new(1, 16);
        router.register_remote_subscription(2, "ch");
        let (_, remote) = router.publish("ch", "1".to_string());
        assert_eq!(remote, 1);

        router.unregister_remote_subscription(2, "ch");
        let _ = router.drain_outbox();
        let (_, remote) = router.publish("ch", "2".to_string());
        assert_eq!(remote, 0);
    }

    #[test]
    fn distributed_pubsub_no_self_forward() {
        let mut router = DistributedPubSubRouter::new(1, 16);
        // Register local node as having subscriptions - should not forward to self.
        router.register_remote_subscription(1, "ch");
        let (_, remote) = router.publish("ch", "msg".to_string());
        assert_eq!(remote, 0);
    }

    #[test]
    fn distributed_pubsub_subscription_snapshot() {
        let mut router = DistributedPubSubRouter::new(1, 16);
        let _rx1 = router.subscribe_local("alpha");
        let _rx2 = router.subscribe_local("beta");
        let mut snap = router.local_subscription_snapshot();
        snap.sort();
        assert_eq!(snap, vec!["alpha", "beta"]);
    }

    // ==============================================================
    // Stream tests
    // ==============================================================

    #[test]
    fn stream_xadd_basic() {
        let mut s = Stream::new();
        let id1 = s.xadd_with_id(
            StreamEntryId::new(1, 0),
            vec![("name".into(), "alice".into())],
        );
        let id2 = s.xadd_with_id(
            StreamEntryId::new(2, 0),
            vec![("name".into(), "bob".into())],
        );
        assert_eq!(id1, StreamEntryId::new(1, 0));
        assert_eq!(id2, StreamEntryId::new(2, 0));
        assert_eq!(s.entries.len(), 2);
        assert_eq!(s.entries[0].fields[0].1, "alice");
        assert_eq!(s.entries[1].fields[0].1, "bob");
    }

    #[test]
    fn stream_xlen() {
        let mut s = Stream::new();
        assert_eq!(s.xlen(), 0);
        s.xadd_with_id(StreamEntryId::new(1, 0), vec![("k".into(), "v".into())]);
        assert_eq!(s.xlen(), 1);
        s.xadd_with_id(StreamEntryId::new(2, 0), vec![("k".into(), "v".into())]);
        assert_eq!(s.xlen(), 2);
    }

    #[test]
    fn stream_xrange() {
        let mut s = Stream::new();
        for i in 1..=5 {
            s.xadd_with_id(
                StreamEntryId::new(i, 0),
                vec![("i".into(), i.to_string())],
            );
        }
        let all = s.xrange(&StreamEntryId::new(1, 0), &StreamEntryId::new(5, 0), None);
        assert_eq!(all.len(), 5);
        let sub = s.xrange(&StreamEntryId::new(2, 0), &StreamEntryId::new(4, 0), None);
        assert_eq!(sub.len(), 3);
        assert_eq!(sub[0].id.ms, 2);
        assert_eq!(sub[2].id.ms, 4);
        let limited = s.xrange(&StreamEntryId::new(1, 0), &StreamEntryId::new(5, 0), Some(2));
        assert_eq!(limited.len(), 2);
    }

    #[test]
    fn stream_xread() {
        let mut s = Stream::new();
        for i in 1..=5 {
            s.xadd_with_id(
                StreamEntryId::new(i, 0),
                vec![("i".into(), i.to_string())],
            );
        }
        let entries = s.xread(&StreamEntryId::new(2, 0), 10);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].id.ms, 3);
        assert_eq!(entries[2].id.ms, 5);
        let entries = s.xread(&StreamEntryId::new(0, 0), 2);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].id.ms, 1);
        assert_eq!(entries[1].id.ms, 2);
    }

    #[test]
    fn stream_xadd_with_max_len() {
        let mut s = Stream::with_max_len(3);
        for i in 1..=5 {
            s.xadd_with_id(
                StreamEntryId::new(i, 0),
                vec![("i".into(), i.to_string())],
            );
        }
        assert_eq!(s.xlen(), 3);
        assert_eq!(s.entries[0].id.ms, 3);
        assert_eq!(s.entries[1].id.ms, 4);
        assert_eq!(s.entries[2].id.ms, 5);
    }

    #[test]
    fn stream_xtrim() {
        let mut s = Stream::new();
        for i in 1..=10 {
            s.xadd_with_id(
                StreamEntryId::new(i, 0),
                vec![("i".into(), i.to_string())],
            );
        }
        assert_eq!(s.xlen(), 10);
        let removed = s.xtrim(7);
        assert_eq!(removed, 3);
        assert_eq!(s.xlen(), 7);
        assert_eq!(s.entries[0].id.ms, 4);
        let removed = s.xtrim(100);
        assert_eq!(removed, 0);
        assert_eq!(s.xlen(), 7);
    }

    #[test]
    fn stream_consumer_group_create() {
        let mut s = Stream::new();
        s.xadd_with_id(StreamEntryId::new(1, 0), vec![("k".into(), "v".into())]);
        s.xgroup_create("mygroup", StreamEntryId::new(0, 0));
        assert!(s.groups.contains_key("mygroup"));
        assert_eq!(s.groups["mygroup"].last_delivered_id, StreamEntryId::new(0, 0));
    }

    #[test]
    fn stream_xreadgroup_basic() {
        let mut s = Stream::new();
        for i in 1..=5 {
            s.xadd_with_id(
                StreamEntryId::new(i, 0),
                vec![("i".into(), i.to_string())],
            );
        }
        s.xgroup_create("grp", StreamEntryId::new(0, 0));
        let entries = s.xreadgroup("grp", "consumer-1", 3);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].id.ms, 1);
        assert_eq!(entries[2].id.ms, 3);
        let entries = s.xreadgroup("grp", "consumer-1", 10);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].id.ms, 4);
        assert_eq!(entries[1].id.ms, 5);
    }

    #[test]
    fn stream_xack() {
        let mut s = Stream::new();
        for i in 1..=3 {
            s.xadd_with_id(
                StreamEntryId::new(i, 0),
                vec![("i".into(), i.to_string())],
            );
        }
        s.xgroup_create("grp", StreamEntryId::new(0, 0));
        let _ = s.xreadgroup("grp", "c1", 3);
        let pending = s.xpending("grp");
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0], ("c1".to_string(), 3));
        let acked = s.xack("grp", &[StreamEntryId::new(1, 0), StreamEntryId::new(2, 0)]);
        assert_eq!(acked, 2);
        let pending = s.xpending("grp");
        assert_eq!(pending[0], ("c1".to_string(), 1));
    }

    #[test]
    fn stream_xpending() {
        let mut s = Stream::new();
        for i in 1..=6 {
            s.xadd_with_id(
                StreamEntryId::new(i, 0),
                vec![("i".into(), i.to_string())],
            );
        }
        s.xgroup_create("grp", StreamEntryId::new(0, 0));
        let _ = s.xreadgroup("grp", "alice", 3);
        let _ = s.xreadgroup("grp", "bob", 3);
        let pending = s.xpending("grp");
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].0, "alice");
        assert_eq!(pending[0].1, 3);
        assert_eq!(pending[1].0, "bob");
        assert_eq!(pending[1].1, 3);
    }

    #[test]
    fn stream_xinfo_groups() {
        let mut s = Stream::new();
        for i in 1..=4 {
            s.xadd_with_id(
                StreamEntryId::new(i, 0),
                vec![("i".into(), i.to_string())],
            );
        }
        s.xgroup_create("g1", StreamEntryId::new(0, 0));
        s.xgroup_create("g2", StreamEntryId::new(0, 0));
        let _ = s.xreadgroup("g1", "c1", 2);
        let _ = s.xreadgroup("g2", "c2", 4);
        let _ = s.xreadgroup("g2", "c3", 0);
        let mut info = s.xinfo_groups();
        info.sort_by_key(|&(name, _, _)| name.to_string());
        assert_eq!(info.len(), 2);
        assert_eq!(info[0].0, "g1");
        assert_eq!(info[0].1, 2);
        assert_eq!(info[0].2, 1);
        assert_eq!(info[1].0, "g2");
        assert_eq!(info[1].1, 4);
        assert_eq!(info[1].2, 2);
    }

    #[test]
    fn stream_multiple_consumers() {
        let mut s = Stream::new();
        for i in 1..=9 {
            s.xadd_with_id(
                StreamEntryId::new(i, 0),
                vec![("i".into(), i.to_string())],
            );
        }
        s.xgroup_create("grp", StreamEntryId::new(0, 0));
        let batch1 = s.xreadgroup("grp", "c1", 3);
        assert_eq!(batch1.len(), 3);
        assert_eq!(batch1[0].id.ms, 1);
        let batch2 = s.xreadgroup("grp", "c2", 3);
        assert_eq!(batch2.len(), 3);
        assert_eq!(batch2[0].id.ms, 4);
        let batch3 = s.xreadgroup("grp", "c3", 3);
        assert_eq!(batch3.len(), 3);
        assert_eq!(batch3[0].id.ms, 7);
        let batch4 = s.xreadgroup("grp", "c1", 10);
        assert_eq!(batch4.len(), 0);
        let pending = s.xpending("grp");
        assert_eq!(pending.len(), 3);
        for (_, count) in &pending {
            assert_eq!(*count, 3);
        }
        s.xack("grp", &[
            StreamEntryId::new(1, 0),
            StreamEntryId::new(2, 0),
            StreamEntryId::new(3, 0),
        ]);
        let pending = s.xpending("grp");
        assert_eq!(pending.len(), 2);
        assert!(pending.iter().all(|(name, _)| name != "c1"));
    }

    #[test]
    fn stream_entry_id_display_and_ordering() {
        let id1 = StreamEntryId::new(100, 0);
        let id2 = StreamEntryId::new(100, 1);
        let id3 = StreamEntryId::new(200, 0);
        assert_eq!(format!("{}", id1), "100-0");
        assert_eq!(format!("{}", id2), "100-1");
        assert!(id1 < id2);
        assert!(id2 < id3);
        assert!(id1 < id3);
    }

    #[test]
    fn stream_auto_id_generation() {
        let mut s = Stream::new();
        let id1 = s.xadd(vec![("k".into(), "v1".into())]);
        let id2 = s.xadd(vec![("k".into(), "v2".into())]);
        assert!(id2 > id1);
        assert_eq!(s.xlen(), 2);
    }

}