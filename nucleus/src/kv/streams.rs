//! Redis Streams implementation for the Nucleus KV store.
//!
//! Implements the core Redis Streams data structure with:
//!   - Stream entries with auto-generated or explicit IDs (`<ms>-<seq>`)
//!   - XADD, XLEN, XRANGE, XREVRANGE, XREAD
//!   - Consumer groups: XGROUP CREATE/DESTROY, XREADGROUP, XACK
//!   - Trimming via XTRIM (MAXLEN)
//!
//! Stream IDs follow Redis format: `<millisecondsTime>-<sequenceNumber>`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

// ============================================================================
// Stream ID
// ============================================================================

/// A Redis-compatible stream entry ID: `<ms>-<seq>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamId {
    pub ms: u64,
    pub seq: u64,
}

impl StreamId {
    pub fn new(ms: u64, seq: u64) -> Self {
        Self { ms, seq }
    }

    /// Parse a stream ID from a string like "1526919030474-0" or "*".
    /// Returns None for "*" (auto-generate) or invalid format.
    pub fn parse(s: &str) -> Option<Self> {
        if s == "*" {
            return None;
        }
        if let Some((ms_str, seq_str)) = s.split_once('-') {
            let ms = ms_str.parse::<u64>().ok()?;
            let seq = seq_str.parse::<u64>().ok()?;
            Some(Self { ms, seq })
        } else {
            // Just a timestamp — seq defaults to 0
            let ms = s.parse::<u64>().ok()?;
            Some(Self { ms, seq: 0 })
        }
    }

    /// The minimum possible stream ID.
    pub fn min() -> Self {
        Self { ms: 0, seq: 0 }
    }

    /// The maximum possible stream ID.
    pub fn max() -> Self {
        Self {
            ms: u64::MAX,
            seq: u64::MAX,
        }
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

// ============================================================================
// Stream Entry
// ============================================================================

/// A single entry in a stream — a list of field-value pairs.
#[derive(Debug, Clone)]
pub struct StreamEntry {
    pub id: StreamId,
    pub fields: Vec<(String, String)>,
}

// ============================================================================
// Consumer Group
// ============================================================================

/// A consumer group tracking read positions and pending entries.
#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    pub name: String,
    /// Last ID delivered to this group.
    pub last_delivered_id: StreamId,
    /// Pending Entry List: entry_id → consumer_name.
    pub pel: HashMap<StreamId, String>,
    /// Per-consumer set of pending entry IDs.
    pub consumers: HashMap<String, HashSet<StreamId>>,
}

impl ConsumerGroup {
    pub fn new(name: String, start_id: StreamId) -> Self {
        Self {
            name,
            last_delivered_id: start_id,
            pel: HashMap::new(),
            consumers: HashMap::new(),
        }
    }
}

// ============================================================================
// Stream
// ============================================================================

/// A Redis-compatible stream.
#[derive(Debug, Clone)]
pub struct Stream {
    /// All entries sorted by ID.
    entries: BTreeMap<StreamId, StreamEntry>,
    /// Last generated ID (for auto-ID generation).
    last_id: StreamId,
    /// Consumer groups.
    groups: HashMap<String, ConsumerGroup>,
}

impl Default for Stream {
    fn default() -> Self {
        Self::new()
    }
}

impl Stream {
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            last_id: StreamId::new(0, 0),
            groups: HashMap::new(),
        }
    }

    /// Number of entries in the stream.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the stream is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Generate the next auto-ID based on current time.
    fn next_auto_id(&self) -> StreamId {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        if now_ms > self.last_id.ms {
            StreamId::new(now_ms, 0)
        } else {
            // Same or earlier millisecond — increment seq
            StreamId::new(self.last_id.ms, self.last_id.seq + 1)
        }
    }

    // ---- XADD ----

    /// Add an entry to the stream. Returns the assigned ID.
    ///
    /// If `id_str` is "*", auto-generates an ID. Otherwise parses the explicit ID.
    /// Returns an error if the explicit ID is not greater than the last ID.
    pub fn xadd(&mut self, id_str: &str, fields: Vec<(String, String)>) -> Result<StreamId, String> {
        let id = if id_str == "*" {
            self.next_auto_id()
        } else {
            let parsed = StreamId::parse(id_str)
                .ok_or_else(|| format!("Invalid stream ID: {id_str}"))?;
            if parsed <= self.last_id {
                return Err("The ID specified in XADD is equal or smaller than the target stream top item".to_string());
            }
            parsed
        };

        let entry = StreamEntry {
            id,
            fields,
        };
        self.entries.insert(id, entry);
        self.last_id = id;
        Ok(id)
    }

    // ---- XLEN ----

    /// Get the number of entries (same as `len()`).
    pub fn xlen(&self) -> usize {
        self.entries.len()
    }

    // ---- XRANGE / XREVRANGE ----

    /// Get entries in the range `[start, end]` (inclusive).
    /// Use "-" for minimum and "+" for maximum.
    pub fn xrange(&self, start: &str, end: &str, count: Option<usize>) -> Vec<&StreamEntry> {
        let start_id = if start == "-" {
            StreamId::min()
        } else {
            StreamId::parse(start).unwrap_or(StreamId::min())
        };
        let end_id = if end == "+" {
            StreamId::max()
        } else {
            StreamId::parse(end).unwrap_or(StreamId::max())
        };

        let mut result: Vec<&StreamEntry> = self
            .entries
            .range(start_id..=end_id)
            .map(|(_, entry)| entry)
            .collect();

        if let Some(c) = count {
            result.truncate(c);
        }
        result
    }

    /// Get entries in reverse order in the range `[end, start]`.
    pub fn xrevrange(&self, end: &str, start: &str, count: Option<usize>) -> Vec<&StreamEntry> {
        let start_id = if start == "-" {
            StreamId::min()
        } else {
            StreamId::parse(start).unwrap_or(StreamId::min())
        };
        let end_id = if end == "+" {
            StreamId::max()
        } else {
            StreamId::parse(end).unwrap_or(StreamId::max())
        };

        let mut result: Vec<&StreamEntry> = self
            .entries
            .range(start_id..=end_id)
            .rev()
            .map(|(_, entry)| entry)
            .collect();

        if let Some(c) = count {
            result.truncate(c);
        }
        result
    }

    // ---- XREAD ----

    /// Read entries with ID strictly greater than `last_id`.
    pub fn xread(&self, last_id: &str, count: Option<usize>) -> Vec<&StreamEntry> {
        let start = if last_id == "$" {
            // $ means "only new entries from now" — return nothing for existing
            return Vec::new();
        } else {
            StreamId::parse(last_id).unwrap_or(StreamId::min())
        };

        let mut result: Vec<&StreamEntry> = self
            .entries
            .range((std::ops::Bound::Excluded(start), std::ops::Bound::Unbounded))
            .map(|(_, entry)| entry)
            .collect();

        if let Some(c) = count {
            result.truncate(c);
        }
        result
    }

    // ---- XDEL ----

    /// Delete entries by ID. Returns the number of entries deleted.
    pub fn xdel(&mut self, ids: &[StreamId]) -> usize {
        let mut count = 0;
        for id in ids {
            if self.entries.remove(id).is_some() {
                count += 1;
            }
        }
        count
    }

    // ---- XTRIM ----

    /// Trim the stream to at most `maxlen` entries (keeps the newest).
    pub fn xtrim_maxlen(&mut self, maxlen: usize) -> usize {
        let current = self.entries.len();
        if current <= maxlen {
            return 0;
        }
        let to_remove = current - maxlen;
        let ids_to_remove: Vec<StreamId> = self
            .entries
            .keys()
            .take(to_remove)
            .copied()
            .collect();
        for id in &ids_to_remove {
            self.entries.remove(id);
        }
        ids_to_remove.len()
    }

    // ---- Consumer Groups ----

    /// Create a consumer group. `start_id` is "0" for beginning, "$" for latest.
    pub fn xgroup_create(&mut self, group_name: &str, start_id: &str) -> Result<(), String> {
        if self.groups.contains_key(group_name) {
            return Err("BUSYGROUP Consumer Group name already exists".to_string());
        }
        let id = if start_id == "$" {
            self.last_id
        } else if start_id == "0" || start_id == "0-0" {
            StreamId::min()
        } else {
            StreamId::parse(start_id)
                .ok_or_else(|| format!("Invalid stream ID: {start_id}"))?
        };
        self.groups
            .insert(group_name.to_string(), ConsumerGroup::new(group_name.to_string(), id));
        Ok(())
    }

    /// Destroy a consumer group. Returns true if it existed.
    pub fn xgroup_destroy(&mut self, group_name: &str) -> bool {
        self.groups.remove(group_name).is_some()
    }

    /// XREADGROUP: read new entries for a consumer in a group.
    ///
    /// If `pending_id` is ">", delivers new (undelivered) entries.
    /// Otherwise re-delivers pending entries from the consumer's PEL.
    pub fn xreadgroup(
        &mut self,
        group_name: &str,
        consumer_name: &str,
        pending_id: &str,
        count: Option<usize>,
    ) -> Result<Vec<StreamEntry>, String> {
        let group = self
            .groups
            .get_mut(group_name)
            .ok_or_else(|| format!("NOGROUP No such consumer group '{group_name}'"))?;

        if pending_id == ">" {
            // Deliver new entries after last_delivered_id
            let start = group.last_delivered_id;
            let entries: Vec<StreamEntry> = self
                .entries
                .range((std::ops::Bound::Excluded(start), std::ops::Bound::Unbounded))
                .map(|(_, e)| e.clone())
                .collect();

            let limited: Vec<StreamEntry> = if let Some(c) = count {
                entries.into_iter().take(c).collect()
            } else {
                entries
            };

            // Update last_delivered_id and add to PEL
            for entry in &limited {
                group.last_delivered_id = entry.id;
                group
                    .pel
                    .insert(entry.id, consumer_name.to_string());
                group
                    .consumers
                    .entry(consumer_name.to_string())
                    .or_default()
                    .insert(entry.id);
            }

            Ok(limited)
        } else {
            // Re-deliver pending entries for this consumer
            let consumer_pending = group
                .consumers
                .get(consumer_name)
                .cloned()
                .unwrap_or_default();

            let mut entries: Vec<StreamEntry> = consumer_pending
                .iter()
                .filter_map(|id| self.entries.get(id).cloned())
                .collect();
            entries.sort_by_key(|e| e.id);

            if let Some(c) = count {
                entries.truncate(c);
            }
            Ok(entries)
        }
    }

    /// Acknowledge entries as processed. Returns the number acknowledged.
    pub fn xack(&mut self, group_name: &str, ids: &[StreamId]) -> Result<usize, String> {
        let group = self
            .groups
            .get_mut(group_name)
            .ok_or_else(|| format!("NOGROUP No such consumer group '{group_name}'"))?;

        let mut count = 0;
        for id in ids {
            if let Some(consumer) = group.pel.remove(id) {
                if let Some(pending_set) = group.consumers.get_mut(&consumer) {
                    pending_set.remove(id);
                }
                count += 1;
            }
        }
        Ok(count)
    }

    /// Get stream info: first/last entry ID, length, groups count.
    pub fn xinfo(&self) -> StreamInfo {
        StreamInfo {
            length: self.entries.len(),
            first_entry: self.entries.keys().next().copied(),
            last_entry: self.entries.keys().next_back().copied(),
            groups: self.groups.len(),
        }
    }
}

/// Summary info about a stream.
#[derive(Debug, Clone)]
pub struct StreamInfo {
    pub length: usize,
    pub first_entry: Option<StreamId>,
    pub last_entry: Option<StreamId>,
    pub groups: usize,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xadd_and_xlen() {
        let mut s = Stream::new();
        let id1 = s.xadd("1-0", vec![("key".into(), "val1".into())]).unwrap();
        assert_eq!(id1, StreamId::new(1, 0));
        let id2 = s.xadd("2-0", vec![("key".into(), "val2".into())]).unwrap();
        assert_eq!(id2, StreamId::new(2, 0));
        assert_eq!(s.xlen(), 2);
    }

    #[test]
    fn xadd_auto_id() {
        let mut s = Stream::new();
        let id = s.xadd("*", vec![("a".into(), "b".into())]).unwrap();
        assert!(id.ms > 0 || id.seq > 0);
        let id2 = s.xadd("*", vec![("c".into(), "d".into())]).unwrap();
        assert!(id2 > id);
        assert_eq!(s.xlen(), 2);
    }

    #[test]
    fn xadd_rejects_lower_id() {
        let mut s = Stream::new();
        s.xadd("5-0", vec![]).unwrap();
        assert!(s.xadd("3-0", vec![]).is_err());
        assert!(s.xadd("5-0", vec![]).is_err()); // equal is also rejected
    }

    #[test]
    fn xrange_basic() {
        let mut s = Stream::new();
        s.xadd("1-0", vec![("a".into(), "1".into())]).unwrap();
        s.xadd("2-0", vec![("a".into(), "2".into())]).unwrap();
        s.xadd("3-0", vec![("a".into(), "3".into())]).unwrap();

        let all = s.xrange("-", "+", None);
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].id, StreamId::new(1, 0));
        assert_eq!(all[2].id, StreamId::new(3, 0));

        let subset = s.xrange("2-0", "3-0", None);
        assert_eq!(subset.len(), 2);

        let limited = s.xrange("-", "+", Some(2));
        assert_eq!(limited.len(), 2);
    }

    #[test]
    fn xrevrange_basic() {
        let mut s = Stream::new();
        s.xadd("1-0", vec![("a".into(), "1".into())]).unwrap();
        s.xadd("2-0", vec![("a".into(), "2".into())]).unwrap();
        s.xadd("3-0", vec![("a".into(), "3".into())]).unwrap();

        let all = s.xrevrange("+", "-", None);
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].id, StreamId::new(3, 0));
        assert_eq!(all[2].id, StreamId::new(1, 0));
    }

    #[test]
    fn xread_after_id() {
        let mut s = Stream::new();
        s.xadd("1-0", vec![("a".into(), "1".into())]).unwrap();
        s.xadd("2-0", vec![("a".into(), "2".into())]).unwrap();
        s.xadd("3-0", vec![("a".into(), "3".into())]).unwrap();

        let entries = s.xread("1-0", None);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].id, StreamId::new(2, 0));

        let from_dollar = s.xread("$", None);
        assert!(from_dollar.is_empty());
    }

    #[test]
    fn xdel_entries() {
        let mut s = Stream::new();
        s.xadd("1-0", vec![]).unwrap();
        s.xadd("2-0", vec![]).unwrap();
        s.xadd("3-0", vec![]).unwrap();
        let deleted = s.xdel(&[StreamId::new(2, 0)]);
        assert_eq!(deleted, 1);
        assert_eq!(s.xlen(), 2);
    }

    #[test]
    fn xtrim_maxlen() {
        let mut s = Stream::new();
        s.xadd("1-0", vec![]).unwrap();
        s.xadd("2-0", vec![]).unwrap();
        s.xadd("3-0", vec![]).unwrap();
        s.xadd("4-0", vec![]).unwrap();
        let trimmed = s.xtrim_maxlen(2);
        assert_eq!(trimmed, 2);
        assert_eq!(s.xlen(), 2);
        // Should keep the newest
        let entries = s.xrange("-", "+", None);
        assert_eq!(entries[0].id, StreamId::new(3, 0));
        assert_eq!(entries[1].id, StreamId::new(4, 0));
    }

    #[test]
    fn consumer_group_basic() {
        let mut s = Stream::new();
        s.xadd("1-0", vec![("msg".into(), "hello".into())]).unwrap();
        s.xadd("2-0", vec![("msg".into(), "world".into())]).unwrap();

        // Create group from beginning
        s.xgroup_create("mygroup", "0").unwrap();

        // Read new entries for consumer "alice"
        let entries = s.xreadgroup("mygroup", "alice", ">", None).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].fields[0].1, "hello");

        // No more new entries
        let entries2 = s.xreadgroup("mygroup", "alice", ">", None).unwrap();
        assert!(entries2.is_empty());

        // Ack the first entry
        let acked = s.xack("mygroup", &[StreamId::new(1, 0)]).unwrap();
        assert_eq!(acked, 1);

        // Re-read pending for alice (using "0" instead of ">")
        let pending = s.xreadgroup("mygroup", "alice", "0", None).unwrap();
        assert_eq!(pending.len(), 1); // only entry 2-0 is still pending
        assert_eq!(pending[0].id, StreamId::new(2, 0));
    }

    #[test]
    fn consumer_group_duplicate_error() {
        let mut s = Stream::new();
        s.xgroup_create("g1", "0").unwrap();
        assert!(s.xgroup_create("g1", "0").is_err());
    }

    #[test]
    fn consumer_group_destroy() {
        let mut s = Stream::new();
        s.xgroup_create("g1", "0").unwrap();
        assert!(s.xgroup_destroy("g1"));
        assert!(!s.xgroup_destroy("g1"));
    }

    #[test]
    fn xinfo_basic() {
        let mut s = Stream::new();
        s.xadd("1-0", vec![("a".into(), "b".into())]).unwrap();
        s.xadd("2-0", vec![("c".into(), "d".into())]).unwrap();
        s.xgroup_create("g1", "0").unwrap();

        let info = s.xinfo();
        assert_eq!(info.length, 2);
        assert_eq!(info.first_entry, Some(StreamId::new(1, 0)));
        assert_eq!(info.last_entry, Some(StreamId::new(2, 0)));
        assert_eq!(info.groups, 1);
    }

    #[test]
    fn stream_id_parse() {
        assert_eq!(StreamId::parse("123-456"), Some(StreamId::new(123, 456)));
        assert_eq!(StreamId::parse("123"), Some(StreamId::new(123, 0)));
        assert_eq!(StreamId::parse("*"), None);
        assert_eq!(StreamId::parse("abc"), None);
    }

    #[test]
    fn stream_id_display() {
        assert_eq!(format!("{}", StreamId::new(100, 5)), "100-5");
    }
}
