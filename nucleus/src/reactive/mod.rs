//! Reactive subscriptions and scheduled tasks engine.
//!
//! Supports:
//!   - Subscribe to SQL queries, receive pushed diffs on result changes
//!   - Scheduled task execution (cron-like at DB level)
//!   - Change notification channels
//!
//! Replaces Supabase Realtime, Debezium+Kafka, pg_cron.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::time::Duration;

// ============================================================================
// Change notifications
// ============================================================================

/// The type of change that occurred.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChangeType {
    Insert,
    Update,
    Delete,
}

/// A row change notification.
#[derive(Debug, Clone)]
pub struct ChangeEvent {
    pub table: String,
    pub change_type: ChangeType,
    /// The row data after the change (None for deletes).
    pub new_row: Option<HashMap<String, String>>,
    /// The row data before the change (None for inserts).
    pub old_row: Option<HashMap<String, String>>,
    pub timestamp: u64,
}

/// Change notification hub — tables emit changes, subscribers receive them.
pub struct ChangeNotifier {
    /// table_name → broadcast sender
    channels: HashMap<String, broadcast::Sender<Arc<ChangeEvent>>>,
    capacity: usize,
}

impl ChangeNotifier {
    pub fn new(capacity: usize) -> Self {
        Self {
            channels: HashMap::new(),
            capacity,
        }
    }

    /// Notify a change on a table.
    pub fn notify(&mut self, event: ChangeEvent) -> usize {
        let table = event.table.clone();
        let event = Arc::new(event);
        if let Some(tx) = self.channels.get(&table) {
            tx.send(event).unwrap_or(0)
        } else {
            0
        }
    }

    /// Subscribe to changes on a table.
    pub fn subscribe(&mut self, table: &str) -> broadcast::Receiver<Arc<ChangeEvent>> {
        let tx = self
            .channels
            .entry(table.to_string())
            .or_insert_with(|| broadcast::channel(self.capacity).0);
        tx.subscribe()
    }

    /// Get subscriber count for a table.
    pub fn subscriber_count(&self, table: &str) -> usize {
        self.channels
            .get(table)
            .map(|tx| tx.receiver_count())
            .unwrap_or(0)
    }
}

// ============================================================================
// Reactive query subscriptions
// ============================================================================

/// A subscription to a query result set.
#[derive(Debug)]
pub struct QuerySubscription {
    pub id: u64,
    pub query: String,
    /// Tables this query depends on.
    pub depends_on: Vec<String>,
    pub active: Arc<AtomicBool>,
}

/// Diff between two result sets.
#[derive(Debug, Clone)]
pub struct QueryDiff {
    pub subscription_id: u64,
    pub added_rows: Vec<HashMap<String, String>>,
    pub removed_rows: Vec<HashMap<String, String>>,
}

/// Reactive subscription manager.
pub struct SubscriptionManager {
    subscriptions: HashMap<u64, QuerySubscription>,
    /// table → list of subscription IDs that depend on it
    table_deps: HashMap<String, Vec<u64>>,
    next_id: AtomicU64,
    /// Channel for sending diffs to listeners
    diff_tx: broadcast::Sender<Arc<QueryDiff>>,
}

impl SubscriptionManager {
    pub fn new(capacity: usize) -> Self {
        let (diff_tx, _) = broadcast::channel(capacity);
        Self {
            subscriptions: HashMap::new(),
            table_deps: HashMap::new(),
            next_id: AtomicU64::new(1),
            diff_tx,
        }
    }

    /// Subscribe to a query. Returns subscription ID and a diff receiver.
    pub fn subscribe(
        &mut self,
        query: &str,
        depends_on: Vec<String>,
    ) -> (u64, broadcast::Receiver<Arc<QueryDiff>>) {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);

        let sub = QuerySubscription {
            id,
            query: query.to_string(),
            depends_on: depends_on.clone(),
            active: Arc::new(AtomicBool::new(true)),
        };

        for table in &depends_on {
            self.table_deps
                .entry(table.clone())
                .or_default()
                .push(id);
        }

        self.subscriptions.insert(id, sub);
        let rx = self.diff_tx.subscribe();
        (id, rx)
    }

    /// Unsubscribe.
    pub fn unsubscribe(&mut self, id: u64) -> bool {
        if let Some(sub) = self.subscriptions.remove(&id) {
            sub.active.store(false, Ordering::Relaxed);
            for table in &sub.depends_on {
                if let Some(deps) = self.table_deps.get_mut(table) {
                    deps.retain(|&d| d != id);
                }
            }
            true
        } else {
            false
        }
    }

    /// Get subscription IDs affected by a table change.
    pub fn affected_subscriptions(&self, table: &str) -> Vec<u64> {
        self.table_deps
            .get(table)
            .cloned()
            .unwrap_or_default()
    }

    /// Push a diff to all listeners.
    pub fn push_diff(&self, diff: QueryDiff) -> usize {
        self.diff_tx.send(Arc::new(diff)).unwrap_or(0)
    }

    /// Number of active subscriptions.
    pub fn active_count(&self) -> usize {
        self.subscriptions
            .values()
            .filter(|s| s.active.load(Ordering::Relaxed))
            .count()
    }
}

// ============================================================================
// Scheduled tasks (cron)
// ============================================================================

/// How often a task should run.
#[derive(Debug, Clone)]
pub enum Schedule {
    /// Run every N seconds.
    EverySeconds(u64),
    /// Run every N minutes.
    EveryMinutes(u64),
    /// Run every N hours.
    EveryHours(u64),
    /// Run once at a specific interval from now.
    Once(Duration),
}

impl Schedule {
    /// Get the duration between runs.
    pub fn duration(&self) -> Duration {
        match self {
            Schedule::EverySeconds(s) => Duration::from_secs(*s),
            Schedule::EveryMinutes(m) => Duration::from_secs(*m * 60),
            Schedule::EveryHours(h) => Duration::from_secs(*h * 3600),
            Schedule::Once(d) => *d,
        }
    }

    /// Whether this is a recurring schedule.
    pub fn is_recurring(&self) -> bool {
        !matches!(self, Schedule::Once(_))
    }
}

/// A scheduled task definition.
#[derive(Debug, Clone)]
pub struct ScheduledTask {
    pub id: u64,
    pub name: String,
    pub sql: String,
    pub schedule: Schedule,
    pub enabled: bool,
    pub last_run: Option<u64>,
    pub run_count: u64,
    pub last_error: Option<String>,
}

/// Scheduled task manager.
pub struct TaskScheduler {
    tasks: HashMap<u64, ScheduledTask>,
    next_id: u64,
}

impl TaskScheduler {
    pub fn new() -> Self {
        Self {
            tasks: HashMap::new(),
            next_id: 1,
        }
    }

    /// Schedule a new task. Returns the task ID.
    pub fn schedule(
        &mut self,
        name: &str,
        sql: &str,
        schedule: Schedule,
    ) -> u64 {
        let id = self.next_id;
        self.next_id += 1;

        self.tasks.insert(
            id,
            ScheduledTask {
                id,
                name: name.to_string(),
                sql: sql.to_string(),
                schedule,
                enabled: true,
                last_run: None,
                run_count: 0,
                last_error: None,
            },
        );

        id
    }

    /// Remove a scheduled task.
    pub fn unschedule(&mut self, id: u64) -> bool {
        self.tasks.remove(&id).is_some()
    }

    /// Enable/disable a task.
    pub fn set_enabled(&mut self, id: u64, enabled: bool) -> bool {
        if let Some(task) = self.tasks.get_mut(&id) {
            task.enabled = enabled;
            true
        } else {
            false
        }
    }

    /// Get tasks that are due to run (based on current time).
    pub fn due_tasks(&self, now_ms: u64) -> Vec<&ScheduledTask> {
        self.tasks
            .values()
            .filter(|t| {
                if !t.enabled {
                    return false;
                }
                match t.last_run {
                    None => true, // Never run, due immediately
                    Some(last) => {
                        let interval = t.schedule.duration().as_millis() as u64;
                        now_ms >= last + interval
                    }
                }
            })
            .collect()
    }

    /// Record that a task ran.
    pub fn record_run(&mut self, id: u64, now_ms: u64, error: Option<String>) {
        if let Some(task) = self.tasks.get_mut(&id) {
            task.last_run = Some(now_ms);
            task.run_count += 1;
            task.last_error = error;

            // Disable one-shot tasks after running
            if !task.schedule.is_recurring() {
                task.enabled = false;
            }
        }
    }

    /// Get a task by ID.
    pub fn get_task(&self, id: u64) -> Option<&ScheduledTask> {
        self.tasks.get(&id)
    }

    /// List all tasks.
    pub fn all_tasks(&self) -> Vec<&ScheduledTask> {
        self.tasks.values().collect()
    }

    /// Number of enabled tasks.
    pub fn enabled_count(&self) -> usize {
        self.tasks.values().filter(|t| t.enabled).count()
    }
}

// ============================================================================
// Change Data Capture (CDC) — Log
// ============================================================================

/// A CDC log entry representing a committed change.
#[derive(Debug, Clone)]
pub struct CdcLogEntry {
    pub sequence: u64,
    pub table: String,
    pub change_type: ChangeType,
    pub row_data: HashMap<String, String>,
    pub timestamp: u64,
}

/// CDC log — ordered log of changes with consumer tracking.
pub struct CdcLog {
    events: Vec<CdcLogEntry>,
    /// consumer_name → last consumed sequence
    consumers: HashMap<String, u64>,
    next_sequence: u64,
}

impl CdcLog {
    pub fn new() -> Self {
        Self {
            events: Vec::new(),
            consumers: HashMap::new(),
            next_sequence: 1,
        }
    }

    /// Append a change event.
    pub fn append(&mut self, table: &str, change_type: ChangeType, row_data: HashMap<String, String>) -> u64 {
        let seq = self.next_sequence;
        self.next_sequence += 1;

        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        self.events.push(CdcLogEntry {
            sequence: seq,
            table: table.to_string(),
            change_type,
            row_data,
            timestamp: ts,
        });

        seq
    }

    /// Read events after a sequence number (for a consumer).
    pub fn read_from(&self, after_sequence: u64, limit: usize) -> Vec<&CdcLogEntry> {
        self.events
            .iter()
            .filter(|e| e.sequence > after_sequence)
            .take(limit)
            .collect()
    }

    /// Read events for a specific table after a sequence number.
    pub fn read_table_from(&self, table: &str, after_sequence: u64, limit: usize) -> Vec<&CdcLogEntry> {
        self.events
            .iter()
            .filter(|e| e.sequence > after_sequence && e.table == table)
            .take(limit)
            .collect()
    }

    /// Register a consumer with its current position.
    pub fn register_consumer(&mut self, name: &str) {
        self.consumers.entry(name.to_string()).or_insert(0);
    }

    /// Get the last consumed sequence for a consumer.
    pub fn consumer_position(&self, name: &str) -> u64 {
        self.consumers.get(name).copied().unwrap_or(0)
    }

    /// Acknowledge events up to a sequence number.
    pub fn acknowledge(&mut self, consumer: &str, sequence: u64) {
        self.consumers.insert(consumer.to_string(), sequence);
    }

    /// Total number of events in the stream.
    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

// ============================================================================
// Change Data Capture (CDC) — Streaming
// ============================================================================

/// A CDC event representing a data change, with full before/after data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CdcEvent {
    Insert {
        table: String,
        row_id: u64,
        data: HashMap<String, String>,
    },
    Update {
        table: String,
        row_id: u64,
        old_data: HashMap<String, String>,
        new_data: HashMap<String, String>,
    },
    Delete {
        table: String,
        row_id: u64,
        old_data: HashMap<String, String>,
    },
}

impl CdcEvent {
    /// Return the table name referenced by this event.
    pub fn table(&self) -> &str {
        match self {
            CdcEvent::Insert { table, .. } => table,
            CdcEvent::Update { table, .. } => table,
            CdcEvent::Delete { table, .. } => table,
        }
    }
}

/// A CDC stream that captures changes for a set of tables.
pub struct CdcStream {
    pub name: String,
    pub tables: HashSet<String>,
    pub events: Vec<CdcEvent>,
    pub cursor: u64,
    pub created_at_ms: u64,
}

/// Manager for multiple CDC streams.
pub struct CdcManager {
    streams: HashMap<String, CdcStream>,
}

impl CdcManager {
    /// Create an empty CDC manager.
    pub fn new() -> Self {
        Self {
            streams: HashMap::new(),
        }
    }

    /// Register a new CDC stream that captures changes for the given tables.
    pub fn create_stream(&mut self, name: &str, tables: HashSet<String>) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        self.streams.insert(
            name.to_string(),
            CdcStream {
                name: name.to_string(),
                tables,
                events: Vec::new(),
                cursor: 0,
                created_at_ms: now,
            },
        );
    }

    /// Remove a CDC stream.
    pub fn drop_stream(&mut self, name: &str) -> bool {
        self.streams.remove(name).is_some()
    }

    /// Emit a CDC event, routing it to all streams whose table set matches.
    pub fn emit(&mut self, event: CdcEvent) {
        let table = event.table().to_string();
        for stream in self.streams.values_mut() {
            if stream.tables.contains(&table) {
                stream.events.push(event.clone());
            }
        }
    }

    /// Poll a stream for events starting at the given cursor position.
    /// Returns the matching events and the new cursor value.
    pub fn poll(&self, stream_name: &str, cursor: u64) -> (Vec<CdcEvent>, u64) {
        match self.streams.get(stream_name) {
            Some(stream) => {
                let start = cursor as usize;
                if start >= stream.events.len() {
                    return (Vec::new(), cursor);
                }
                let events: Vec<CdcEvent> = stream.events[start..].to_vec();
                let new_cursor = stream.events.len() as u64;
                (events, new_cursor)
            }
            None => (Vec::new(), cursor),
        }
    }

    /// Number of active streams.
    pub fn stream_count(&self) -> usize {
        self.streams.len()
    }

    /// Look up a stream by name.
    pub fn get_stream(&self, name: &str) -> Option<&CdcStream> {
        self.streams.get(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_row(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[tokio::test]
    async fn change_notification() {
        let mut notifier = ChangeNotifier::new(16);
        let mut rx = notifier.subscribe("orders");

        assert_eq!(notifier.subscriber_count("orders"), 1);

        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        notifier.notify(ChangeEvent {
            table: "orders".into(),
            change_type: ChangeType::Insert,
            new_row: Some(make_row(&[("id", "1"), ("amount", "100")])),
            old_row: None,
            timestamp: ts,
        });

        let event = rx.recv().await.unwrap();
        assert_eq!(event.table, "orders");
        assert_eq!(event.change_type, ChangeType::Insert);
        assert_eq!(event.new_row.as_ref().unwrap()["amount"], "100");
    }

    #[tokio::test]
    async fn subscription_manager() {
        let mut mgr = SubscriptionManager::new(16);

        let (id1, _rx1) = mgr.subscribe(
            "SELECT * FROM orders WHERE status = 'pending'",
            vec!["orders".into()],
        );
        let (_id2, _rx2) = mgr.subscribe(
            "SELECT count(*) FROM users",
            vec!["users".into()],
        );

        assert_eq!(mgr.active_count(), 2);

        // Check affected subscriptions
        let affected = mgr.affected_subscriptions("orders");
        assert_eq!(affected, vec![id1]);

        // Unsubscribe
        assert!(mgr.unsubscribe(id1));
        assert_eq!(mgr.active_count(), 1);
    }

    #[test]
    fn scheduled_tasks() {
        let mut scheduler = TaskScheduler::new();

        let id = scheduler.schedule(
            "cleanup",
            "DELETE FROM sessions WHERE expired_at < now()",
            Schedule::EveryHours(1),
        );

        assert_eq!(scheduler.enabled_count(), 1);

        // Task is due (never run)
        let due = scheduler.due_tasks(1000);
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].name, "cleanup");

        // Record a run
        scheduler.record_run(id, 1000, None);
        let task = scheduler.get_task(id).unwrap();
        assert_eq!(task.run_count, 1);
        assert_eq!(task.last_run, Some(1000));

        // Not due yet (only 30 minutes later)
        let due = scheduler.due_tasks(1000 + 30 * 60 * 1000);
        assert_eq!(due.len(), 0);

        // Due after 1 hour
        let due = scheduler.due_tasks(1000 + 60 * 60 * 1000);
        assert_eq!(due.len(), 1);
    }

    #[test]
    fn one_shot_task() {
        let mut scheduler = TaskScheduler::new();
        let id = scheduler.schedule(
            "migrate",
            "ALTER TABLE users ADD COLUMN avatar TEXT",
            Schedule::Once(Duration::from_secs(0)),
        );

        assert_eq!(scheduler.enabled_count(), 1);
        scheduler.record_run(id, 1000, None);

        // One-shot task should be disabled after running
        assert_eq!(scheduler.enabled_count(), 0);
        assert!(!scheduler.get_task(id).unwrap().enabled);
    }

    #[test]
    fn cdc_log() {
        let mut cdc = CdcLog::new();

        let _s1 = cdc.append("users", ChangeType::Insert, make_row(&[("id", "1"), ("name", "Alice")]));
        let s2 = cdc.append("orders", ChangeType::Insert, make_row(&[("id", "1"), ("user_id", "1")]));
        let _s3 = cdc.append("users", ChangeType::Update, make_row(&[("id", "1"), ("name", "Alice B")]));

        assert_eq!(cdc.len(), 3);

        // Read all from beginning
        let events = cdc.read_from(0, 100);
        assert_eq!(events.len(), 3);

        // Read only users table
        let user_events = cdc.read_table_from("users", 0, 100);
        assert_eq!(user_events.len(), 2);

        // Consumer tracking
        cdc.register_consumer("app1");
        assert_eq!(cdc.consumer_position("app1"), 0);

        cdc.acknowledge("app1", s2);
        assert_eq!(cdc.consumer_position("app1"), s2);

        // Read from consumer position
        let pending = cdc.read_from(cdc.consumer_position("app1"), 100);
        assert_eq!(pending.len(), 1); // Only s3
        assert_eq!(pending[0].table, "users");
    }

    #[test]
    fn disable_enable_task() {
        let mut scheduler = TaskScheduler::new();
        let id = scheduler.schedule(
            "report",
            "SELECT generate_report()",
            Schedule::EveryHours(24),
        );

        assert_eq!(scheduler.enabled_count(), 1);
        scheduler.set_enabled(id, false);
        assert_eq!(scheduler.enabled_count(), 0);

        // Disabled tasks are not due
        let due = scheduler.due_tasks(u64::MAX);
        assert_eq!(due.len(), 0);

        scheduler.set_enabled(id, true);
        assert_eq!(scheduler.enabled_count(), 1);
    }

    // ====================================================================
    // Additional reactive tests
    // ====================================================================

    fn make_change_event(table: &str, change_type: ChangeType) -> ChangeEvent {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        ChangeEvent {
            table: table.into(),
            change_type,
            new_row: Some(make_row(&[("id", "1")])),
            old_row: None,
            timestamp: ts,
        }
    }
    #[tokio::test]
    async fn change_notifier_multiple_tables() {
        let mut notifier = ChangeNotifier::new(16);
        let mut rx_orders = notifier.subscribe("orders");
        let mut rx_users = notifier.subscribe("users");

        assert_eq!(notifier.subscriber_count("orders"), 1);
        assert_eq!(notifier.subscriber_count("users"), 1);

        notifier.notify(make_change_event("orders", ChangeType::Insert));
        notifier.notify(make_change_event("users", ChangeType::Update));

        let evt_orders = rx_orders.recv().await.unwrap();
        let evt_users = rx_users.recv().await.unwrap();

        assert_eq!(evt_orders.table, "orders");
        assert_eq!(evt_orders.change_type, ChangeType::Insert);
        assert_eq!(evt_users.table, "users");
        assert_eq!(evt_users.change_type, ChangeType::Update);
    }

    #[tokio::test]
    async fn change_notifier_filter_by_event_type() {
        let mut notifier = ChangeNotifier::new(16);
        let mut rx = notifier.subscribe("products");

        notifier.notify(make_change_event("products", ChangeType::Insert));

        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        notifier.notify(ChangeEvent {
            table: "products".into(),
            change_type: ChangeType::Update,
            new_row: Some(make_row(&[("id", "1"), ("price", "20")])),
            old_row: Some(make_row(&[("id", "1"), ("price", "10")])),
            timestamp: ts,
        });
        notifier.notify(ChangeEvent {
            table: "products".into(),
            change_type: ChangeType::Delete,
            new_row: None,
            old_row: Some(make_row(&[("id", "1")])),
            timestamp: ts,
        });

        let e1 = rx.recv().await.unwrap();
        let e2 = rx.recv().await.unwrap();
        let e3 = rx.recv().await.unwrap();

        assert_eq!(e1.change_type, ChangeType::Insert);
        assert!(e1.new_row.is_some());
        assert!(e1.old_row.is_none());

        assert_eq!(e2.change_type, ChangeType::Update);
        assert!(e2.new_row.is_some());
        assert!(e2.old_row.is_some());

        assert_eq!(e3.change_type, ChangeType::Delete);
        assert!(e3.new_row.is_none());
        assert!(e3.old_row.is_some());
    }

    #[test]
    fn change_notifier_notify_with_no_subscribers() {
        let mut notifier = ChangeNotifier::new(16);
        let count = notifier.notify(make_change_event("phantom", ChangeType::Insert));
        assert_eq!(count, 0);
    }
    #[test]
    fn subscription_manager_lifecycle() {
        let mut mgr = SubscriptionManager::new(16);
        assert_eq!(mgr.active_count(), 0);

        let (id1, _rx1) = mgr.subscribe(
            "SELECT * FROM orders",
            vec!["orders".into()],
        );
        let (id2, _rx2) = mgr.subscribe(
            "SELECT sum(amount) FROM orders",
            vec!["orders".into()],
        );
        assert_eq!(mgr.active_count(), 2);

        let mut affected = mgr.affected_subscriptions("orders");
        affected.sort();
        let mut expected = vec![id1, id2];
        expected.sort();
        assert_eq!(affected, expected);

        assert!(mgr.unsubscribe(id1));
        assert_eq!(mgr.active_count(), 1);

        let affected = mgr.affected_subscriptions("orders");
        assert_eq!(affected, vec![id2]);

        assert!(mgr.unsubscribe(id2));
        assert_eq!(mgr.active_count(), 0);
        assert!(mgr.affected_subscriptions("orders").is_empty());
    }

    #[test]
    fn subscription_manager_unsubscribe_nonexistent() {
        let mut mgr = SubscriptionManager::new(16);
        assert!(!mgr.unsubscribe(999));
        assert!(!mgr.unsubscribe(0));
    }

    #[test]
    fn subscription_manager_multi_table_deps() {
        let mut mgr = SubscriptionManager::new(16);

        let (id_join, _rx) = mgr.subscribe(
            "SELECT * FROM orders JOIN users ON orders.user_id = users.id",
            vec!["orders".into(), "users".into()],
        );

        let (id_users, _rx2) = mgr.subscribe(
            "SELECT * FROM users",
            vec!["users".into()],
        );

        let affected_orders = mgr.affected_subscriptions("orders");
        assert_eq!(affected_orders, vec![id_join]);

        let mut affected_users = mgr.affected_subscriptions("users");
        affected_users.sort();
        let mut expected = vec![id_join, id_users];
        expected.sort();
        assert_eq!(affected_users, expected);

        assert!(mgr.affected_subscriptions("products").is_empty());
    }
    #[tokio::test]
    async fn subscription_manager_push_diff() {
        let mut mgr = SubscriptionManager::new(16);
        let (id, mut rx) = mgr.subscribe(
            "SELECT * FROM items",
            vec!["items".into()],
        );

        let diff = QueryDiff {
            subscription_id: id,
            added_rows: vec![make_row(&[("id", "5"), ("name", "widget")])],
            removed_rows: vec![make_row(&[("id", "3"), ("name", "gadget")])],
        };
        let listeners = mgr.push_diff(diff);
        assert_eq!(listeners, 1);

        let received = rx.recv().await.unwrap();
        assert_eq!(received.subscription_id, id);
        assert_eq!(received.added_rows.len(), 1);
        assert_eq!(received.added_rows[0]["name"], "widget");
        assert_eq!(received.removed_rows.len(), 1);
        assert_eq!(received.removed_rows[0]["name"], "gadget");
    }

    #[test]
    fn subscription_manager_active_flag() {
        let mut mgr = SubscriptionManager::new(16);
        let (id, _rx) = mgr.subscribe(
            "SELECT 1",
            vec!["t".into()],
        );

        assert_eq!(mgr.active_count(), 1);

        mgr.unsubscribe(id);
        assert_eq!(mgr.active_count(), 0);
    }

    #[tokio::test]
    async fn change_notifier_multiple_subscribers_one_table() {
        let mut notifier = ChangeNotifier::new(16);
        let mut rx1 = notifier.subscribe("logs");
        let mut rx2 = notifier.subscribe("logs");
        let mut rx3 = notifier.subscribe("logs");

        assert_eq!(notifier.subscriber_count("logs"), 3);

        let count = notifier.notify(make_change_event("logs", ChangeType::Insert));
        assert_eq!(count, 3);

        let e1 = rx1.recv().await.unwrap();
        let e2 = rx2.recv().await.unwrap();
        let e3 = rx3.recv().await.unwrap();

        assert!(Arc::ptr_eq(&e1, &e2));
        assert!(Arc::ptr_eq(&e2, &e3));
        assert_eq!(e1.table, "logs");
    }

    #[test]
    fn change_notifier_subscriber_count_after_drop() {
        let mut notifier = ChangeNotifier::new(16);
        let rx1 = notifier.subscribe("data");
        let _rx2 = notifier.subscribe("data");
        assert_eq!(notifier.subscriber_count("data"), 2);

        drop(rx1);
        assert_eq!(notifier.subscriber_count("data"), 1);
    }

    // ====================================================================
    // CDC Streaming tests
    // ====================================================================

    fn tables(names: &[&str]) -> HashSet<String> {
        names.iter().map(|n| n.to_string()).collect()
    }

    #[test]
    fn test_cdc_create_and_drop_stream() {
        let mut mgr = CdcManager::new();
        assert_eq!(mgr.stream_count(), 0);

        mgr.create_stream("stream1", tables(&["orders", "users"]));
        assert_eq!(mgr.stream_count(), 1);

        let stream = mgr.get_stream("stream1").unwrap();
        assert_eq!(stream.name, "stream1");
        assert!(stream.tables.contains("orders"));
        assert!(stream.tables.contains("users"));
        assert_eq!(stream.events.len(), 0);
        assert_eq!(stream.cursor, 0);
        assert!(stream.created_at_ms > 0);

        // Drop the stream
        assert!(mgr.drop_stream("stream1"));
        assert_eq!(mgr.stream_count(), 0);
        assert!(mgr.get_stream("stream1").is_none());

        // Dropping a non-existent stream returns false
        assert!(!mgr.drop_stream("stream1"));
    }

    #[test]
    fn test_cdc_emit_routes_by_table() {
        let mut mgr = CdcManager::new();
        mgr.create_stream("orders_stream", tables(&["orders"]));
        mgr.create_stream("users_stream", tables(&["users"]));

        // Emit an event for "orders" — should only go to orders_stream
        mgr.emit(CdcEvent::Insert {
            table: "orders".into(),
            row_id: 1,
            data: make_row(&[("id", "1"), ("amount", "50")]),
        });

        // Emit an event for "users" — should only go to users_stream
        mgr.emit(CdcEvent::Insert {
            table: "users".into(),
            row_id: 10,
            data: make_row(&[("id", "10"), ("name", "Alice")]),
        });

        // Emit an event for "products" — should go nowhere
        mgr.emit(CdcEvent::Insert {
            table: "products".into(),
            row_id: 100,
            data: make_row(&[("id", "100")]),
        });

        assert_eq!(mgr.get_stream("orders_stream").unwrap().events.len(), 1);
        assert_eq!(mgr.get_stream("users_stream").unwrap().events.len(), 1);
    }

    #[test]
    fn test_cdc_poll_with_cursor() {
        let mut mgr = CdcManager::new();
        mgr.create_stream("s1", tables(&["orders"]));

        // Emit 3 events
        for i in 1..=3 {
            mgr.emit(CdcEvent::Insert {
                table: "orders".into(),
                row_id: i,
                data: make_row(&[("id", &i.to_string())]),
            });
        }

        // Poll from cursor 0 — get all 3
        let (events, cursor) = mgr.poll("s1", 0);
        assert_eq!(events.len(), 3);
        assert_eq!(cursor, 3);

        // Poll from new cursor — nothing new
        let (events2, cursor2) = mgr.poll("s1", cursor);
        assert_eq!(events2.len(), 0);
        assert_eq!(cursor2, 3);

        // Emit one more
        mgr.emit(CdcEvent::Insert {
            table: "orders".into(),
            row_id: 4,
            data: make_row(&[("id", "4")]),
        });

        // Poll from old cursor — get the new event
        let (events3, cursor3) = mgr.poll("s1", cursor);
        assert_eq!(events3.len(), 1);
        assert_eq!(cursor3, 4);

        // Poll non-existent stream
        let (events_none, cursor_none) = mgr.poll("nonexistent", 0);
        assert!(events_none.is_empty());
        assert_eq!(cursor_none, 0);
    }

    #[test]
    fn test_cdc_multiple_streams() {
        let mut mgr = CdcManager::new();

        // Two streams both watching "orders"
        mgr.create_stream("analytics", tables(&["orders"]));
        mgr.create_stream("audit", tables(&["orders", "users"]));

        assert_eq!(mgr.stream_count(), 2);

        // Emit an orders event — both streams should get it
        mgr.emit(CdcEvent::Insert {
            table: "orders".into(),
            row_id: 1,
            data: make_row(&[("id", "1")]),
        });

        assert_eq!(mgr.get_stream("analytics").unwrap().events.len(), 1);
        assert_eq!(mgr.get_stream("audit").unwrap().events.len(), 1);

        // Emit a users event — only audit should get it
        mgr.emit(CdcEvent::Insert {
            table: "users".into(),
            row_id: 2,
            data: make_row(&[("id", "2")]),
        });

        assert_eq!(mgr.get_stream("analytics").unwrap().events.len(), 1);
        assert_eq!(mgr.get_stream("audit").unwrap().events.len(), 2);

        // Each stream has independent cursors via poll
        let (a_events, a_cursor) = mgr.poll("analytics", 0);
        let (b_events, b_cursor) = mgr.poll("audit", 0);
        assert_eq!(a_events.len(), 1);
        assert_eq!(a_cursor, 1);
        assert_eq!(b_events.len(), 2);
        assert_eq!(b_cursor, 2);
    }

    #[test]
    fn test_cdc_event_types() {
        let mut mgr = CdcManager::new();
        mgr.create_stream("all_changes", tables(&["products"]));

        // Insert
        mgr.emit(CdcEvent::Insert {
            table: "products".into(),
            row_id: 1,
            data: make_row(&[("id", "1"), ("price", "10")]),
        });

        // Update
        mgr.emit(CdcEvent::Update {
            table: "products".into(),
            row_id: 1,
            old_data: make_row(&[("id", "1"), ("price", "10")]),
            new_data: make_row(&[("id", "1"), ("price", "20")]),
        });

        // Delete
        mgr.emit(CdcEvent::Delete {
            table: "products".into(),
            row_id: 1,
            old_data: make_row(&[("id", "1"), ("price", "20")]),
        });

        let (events, cursor) = mgr.poll("all_changes", 0);
        assert_eq!(events.len(), 3);
        assert_eq!(cursor, 3);

        // Verify event types
        assert!(matches!(&events[0], CdcEvent::Insert { row_id: 1, .. }));
        assert!(matches!(&events[1], CdcEvent::Update { row_id: 1, .. }));
        assert!(matches!(&events[2], CdcEvent::Delete { row_id: 1, .. }));

        // Verify data in update event
        if let CdcEvent::Update { old_data, new_data, .. } = &events[1] {
            assert_eq!(old_data["price"], "10");
            assert_eq!(new_data["price"], "20");
        } else {
            panic!("Expected Update event");
        }

        // Verify table helper method
        assert_eq!(events[0].table(), "products");
        assert_eq!(events[1].table(), "products");
        assert_eq!(events[2].table(), "products");
    }
}
