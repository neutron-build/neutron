//! Redis-compatible collection data structures for the Nucleus KV store.
//!
//! Provides Lists (VecDeque), Hashes (HashMap), Sets (HashSet), plus delegation
//! to the existing SortedSet and HyperLogLog types. All collections are stored
//! in a 64-shard concurrent map for high-throughput parallel access.

use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::types::Value;

use super::{HyperLogLog, SortedSet, SortedSetEntry};
#[cfg(feature = "server")]
use super::collections_wal::CollectionWal;
use super::streams::Stream;

// ============================================================================
// GeoSet — stores members with (longitude, latitude) backed by the geo R-tree
// ============================================================================

/// A geospatial set: member name → (longitude, latitude), backed by an R-tree
/// for radius queries.
#[derive(Debug)]
pub struct GeoSet {
    /// member → (lon, lat)
    members: HashMap<String, (f64, f64)>,
    /// R-tree index. doc_id == hash of member name.
    tree: crate::geo::RTree,
    /// Forward map: member → doc_id used in the R-tree.
    member_ids: HashMap<String, u64>,
    next_id: u64,
}

impl Clone for GeoSet {
    fn clone(&self) -> Self {
        let mut new = GeoSet::new();
        for (member, &(lon, lat)) in &self.members {
            new.add(lon, lat, member);
        }
        new
    }
}

impl GeoSet {
    pub fn new() -> Self {
        Self {
            members: HashMap::new(),
            tree: crate::geo::RTree::new(),
            member_ids: HashMap::new(),
            next_id: 1,
        }
    }

    /// Add a member with (longitude, latitude). Returns true if the member is
    /// new, false if it was updated.
    pub fn add(&mut self, lon: f64, lat: f64, member: &str) -> bool {
        let is_new = !self.members.contains_key(member);
        self.members.insert(member.to_string(), (lon, lat));
        // For simplicity, always insert into the R-tree (duplicates are fine
        // for search — we deduplicate on read via the members map).
        let id = if let Some(&existing_id) = self.member_ids.get(member) {
            existing_id
        } else {
            let id = self.next_id;
            self.next_id += 1;
            self.member_ids.insert(member.to_string(), id);
            id
        };
        let point = crate::geo::Point::new(lon, lat);
        self.tree.insert(&point, id);
        is_new
    }

    /// Get the position of a member. Returns (lon, lat) or None.
    pub fn pos(&self, member: &str) -> Option<(f64, f64)> {
        self.members.get(member).copied()
    }

    /// Compute the distance between two members in the specified unit.
    /// Returns None if either member does not exist.
    pub fn dist(&self, member1: &str, member2: &str, unit: &str) -> Option<f64> {
        let &(lon1, lat1) = self.members.get(member1)?;
        let &(lon2, lat2) = self.members.get(member2)?;
        let a = crate::geo::Point::new(lon1, lat1);
        let b = crate::geo::Point::new(lon2, lat2);
        let meters = crate::geo::haversine_distance(&a, &b);
        Some(convert_meters(meters, unit))
    }

    /// Find all members within `radius` of (lon, lat) in the given unit.
    /// Returns (member, distance) pairs sorted by distance.
    pub fn radius(
        &self,
        lon: f64,
        lat: f64,
        radius: f64,
        unit: &str,
    ) -> Vec<(String, f64)> {
        let radius_m = convert_to_meters(radius, unit);
        let center = crate::geo::Point::new(lon, lat);
        let doc_ids = self.tree.search_radius(&center, radius_m);

        // Reverse map: doc_id → member name
        let id_to_member: HashMap<u64, &str> = self
            .member_ids
            .iter()
            .map(|(m, &id)| (id, m.as_str()))
            .collect();

        let mut results: Vec<(String, f64)> = doc_ids
            .into_iter()
            .filter_map(|id| {
                let member = id_to_member.get(&id)?;
                let &(mlon, mlat) = self.members.get(*member)?;
                let point = crate::geo::Point::new(mlon, mlat);
                let dist_m = crate::geo::haversine_distance(&center, &point);
                if dist_m <= radius_m {
                    Some((member.to_string(), convert_meters(dist_m, unit)))
                } else {
                    None
                }
            })
            .collect();

        // Deduplicate (R-tree may have stale entries after updates)
        results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        results.dedup_by(|a, b| a.0 == b.0);
        results
    }

    /// Number of members.
    pub fn len(&self) -> usize {
        self.members.len()
    }

    /// Return a reference to the underlying members map.
    pub fn members(&self) -> &HashMap<String, (f64, f64)> {
        &self.members
    }
}

/// Convert meters to the given unit.
fn convert_meters(meters: f64, unit: &str) -> f64 {
    match unit {
        "km" => meters / 1000.0,
        "mi" => meters / 1609.344,
        "ft" => meters * 3.28084,
        _ => meters, // "m" or default
    }
}

/// Convert a value in the given unit to meters.
fn convert_to_meters(value: f64, unit: &str) -> f64 {
    match unit {
        "km" => value * 1000.0,
        "mi" => value * 1609.344,
        "ft" => value / 3.28084,
        _ => value, // "m" or default
    }
}

const NUM_SHARDS: usize = 64;

// ============================================================================
// Collection enum
// ============================================================================

/// A Redis-compatible collection stored per-key.
#[derive(Debug)]
pub enum KvCollection {
    List(VecDeque<Value>),
    Hash(HashMap<String, Value>),
    Set(HashSet<String>),
    SortedSet(SortedSet),
    HyperLogLog(HyperLogLog),
    Stream(Stream),
    Geo(GeoSet),
}

/// Error returned when a key exists but holds the wrong collection type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WrongTypeError {
    pub expected: &'static str,
    pub actual: &'static str,
}

impl std::fmt::Display for WrongTypeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "WRONGTYPE Operation against a key holding the wrong kind of value \
             (expected {}, got {})",
            self.expected, self.actual
        )
    }
}

impl std::error::Error for WrongTypeError {}

impl KvCollection {
    fn type_name(&self) -> &'static str {
        match self {
            KvCollection::List(_) => "list",
            KvCollection::Hash(_) => "hash",
            KvCollection::Set(_) => "set",
            KvCollection::SortedSet(_) => "zset",
            KvCollection::HyperLogLog(_) => "hyperloglog",
            KvCollection::Stream(_) => "stream",
            KvCollection::Geo(_) => "geo",
        }
    }
}

// ============================================================================
// Sharded storage
// ============================================================================

struct CollectionShard {
    data: RwLock<HashMap<String, KvCollection>>,
}

impl CollectionShard {
    fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
        }
    }
}

impl std::fmt::Debug for CollectionShard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let data = self.data.read();
        f.debug_struct("CollectionShard")
            .field("keys", &data.len())
            .finish()
    }
}

/// Sharded collection store using 64 shards for parallel access.
///
/// Each key maps to exactly one [`KvCollection`]. Operations auto-create the
/// appropriate collection on first write and return [`WrongTypeError`] if a key
/// exists but holds a different collection type.
pub struct ShardedCollections {
    shards: Vec<CollectionShard>,
    #[cfg(feature = "server")]
    wal: Option<Arc<CollectionWal>>,
}

impl std::fmt::Debug for ShardedCollections {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardedCollections")
            .field("shards", &self.shards)
            .field("wal_enabled", &{
                #[cfg(feature = "server")]
                { self.wal.is_some() }
                #[cfg(not(feature = "server"))]
                { false }
            })
            .finish()
    }
}

impl Default for ShardedCollections {
    fn default() -> Self {
        Self::new()
    }
}

impl ShardedCollections {
    /// Create a new empty sharded collection store (no WAL).
    pub fn new() -> Self {
        let mut shards = Vec::with_capacity(NUM_SHARDS);
        for _ in 0..NUM_SHARDS {
            shards.push(CollectionShard::new());
        }
        Self {
            shards,
            #[cfg(feature = "server")]
            wal: None,
        }
    }

    /// Attach a WAL to this collection store (called after WAL replay).
    #[cfg(feature = "server")]
    pub fn set_wal(&mut self, wal: Arc<CollectionWal>) {
        self.wal = Some(wal);
    }

    /// Determine shard index for a given key.
    fn shard_index(key: &str) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish() as usize % NUM_SHARDS
    }

    /// Get a reference to the shard for a given key.
    fn shard(&self, key: &str) -> &CollectionShard {
        &self.shards[Self::shard_index(key)]
    }

    // ========================================================================
    // List operations
    // ========================================================================

    /// LPUSH -- push a value to the front of the list at `key`.
    /// Creates the list if it does not exist. Returns the new length.
    pub fn lpush(&self, key: &str, value: Value) -> Result<usize, WrongTypeError> {
        #[cfg(feature = "server")]
        if let Some(ref wal) = self.wal
            && let Err(e) = wal.log_lpush(key, &value) {
                tracing::error!(target: "nucleus::kv::wal", "WAL write failed: {e}");
            }
        let shard = self.shard(key);
        let mut data = shard.data.write();
        let entry = data
            .entry(key.to_string())
            .or_insert_with(|| KvCollection::List(VecDeque::new()));
        match entry {
            KvCollection::List(list) => {
                list.push_front(value);
                Ok(list.len())
            }
            other => Err(WrongTypeError {
                expected: "list",
                actual: other.type_name(),
            }),
        }
    }

    /// RPUSH -- push a value to the back of the list at `key`.
    /// Creates the list if it does not exist. Returns the new length.
    pub fn rpush(&self, key: &str, value: Value) -> Result<usize, WrongTypeError> {
        #[cfg(feature = "server")]
        if let Some(ref wal) = self.wal
            && let Err(e) = wal.log_rpush(key, &value) {
                tracing::error!(target: "nucleus::kv::wal", "WAL write failed: {e}");
            }
        let shard = self.shard(key);
        let mut data = shard.data.write();
        let entry = data
            .entry(key.to_string())
            .or_insert_with(|| KvCollection::List(VecDeque::new()));
        match entry {
            KvCollection::List(list) => {
                list.push_back(value);
                Ok(list.len())
            }
            other => Err(WrongTypeError {
                expected: "list",
                actual: other.type_name(),
            }),
        }
    }

    /// LPOP -- remove and return the first element of the list at `key`.
    pub fn lpop(&self, key: &str) -> Result<Option<Value>, WrongTypeError> {
        #[cfg(feature = "server")]
        if let Some(ref wal) = self.wal
            && let Err(e) = wal.log_lpop(key) {
                tracing::error!(target: "nucleus::kv::wal", "WAL write failed: {e}");
            }
        let shard = self.shard(key);
        let mut data = shard.data.write();
        match data.get_mut(key) {
            Some(KvCollection::List(list)) => {
                let val = list.pop_front();
                if list.is_empty() {
                    data.remove(key);
                }
                Ok(val)
            }
            Some(other) => Err(WrongTypeError {
                expected: "list",
                actual: other.type_name(),
            }),
            None => Ok(None),
        }
    }

    /// RPOP -- remove and return the last element of the list at `key`.
    pub fn rpop(&self, key: &str) -> Result<Option<Value>, WrongTypeError> {
        #[cfg(feature = "server")]
        if let Some(ref wal) = self.wal
            && let Err(e) = wal.log_rpop(key) {
                tracing::error!(target: "nucleus::kv::wal", "WAL write failed: {e}");
            }
        let shard = self.shard(key);
        let mut data = shard.data.write();
        match data.get_mut(key) {
            Some(KvCollection::List(list)) => {
                let val = list.pop_back();
                if list.is_empty() {
                    data.remove(key);
                }
                Ok(val)
            }
            Some(other) => Err(WrongTypeError {
                expected: "list",
                actual: other.type_name(),
            }),
            None => Ok(None),
        }
    }

    /// LRANGE -- return elements from index `start` to `stop` (inclusive).
    /// Negative indices count from the end (-1 = last element).
    pub fn lrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<Value>, WrongTypeError> {
        let shard = self.shard(key);
        let data = shard.data.read();
        match data.get(key) {
            Some(KvCollection::List(list)) => {
                let len = list.len() as i64;
                let s = normalize_index(start, len);
                let e = normalize_index(stop, len);
                if s > e || s >= len as usize {
                    return Ok(vec![]);
                }
                let end = e.min(len as usize - 1);
                Ok(list.iter().skip(s).take(end - s + 1).cloned().collect())
            }
            Some(other) => Err(WrongTypeError {
                expected: "list",
                actual: other.type_name(),
            }),
            None => Ok(vec![]),
        }
    }

    /// LLEN -- return the length of the list at `key`.
    pub fn llen(&self, key: &str) -> Result<usize, WrongTypeError> {
        let shard = self.shard(key);
        let data = shard.data.read();
        match data.get(key) {
            Some(KvCollection::List(list)) => Ok(list.len()),
            Some(other) => Err(WrongTypeError {
                expected: "list",
                actual: other.type_name(),
            }),
            None => Ok(0),
        }
    }

    /// LINDEX -- return the element at `index` in the list.
    /// Negative indices count from the end.
    pub fn lindex(&self, key: &str, index: i64) -> Result<Option<Value>, WrongTypeError> {
        let shard = self.shard(key);
        let data = shard.data.read();
        match data.get(key) {
            Some(KvCollection::List(list)) => {
                let len = list.len() as i64;
                let idx = normalize_index(index, len);
                if idx >= list.len() {
                    Ok(None)
                } else {
                    Ok(list.get(idx).cloned())
                }
            }
            Some(other) => Err(WrongTypeError {
                expected: "list",
                actual: other.type_name(),
            }),
            None => Ok(None),
        }
    }

    // ========================================================================
    // Hash operations
    // ========================================================================

    /// HSET -- set a field in the hash at `key`. Returns true if the field is new.
    pub fn hset(
        &self,
        key: &str,
        field: &str,
        value: Value,
    ) -> Result<bool, WrongTypeError> {
        #[cfg(feature = "server")]
        if let Some(ref wal) = self.wal
            && let Err(e) = wal.log_hset(key, field, &value) {
                tracing::error!(target: "nucleus::kv::wal", "WAL write failed: {e}");
            }
        let shard = self.shard(key);
        let mut data = shard.data.write();
        let entry = data
            .entry(key.to_string())
            .or_insert_with(|| KvCollection::Hash(HashMap::new()));
        match entry {
            KvCollection::Hash(hash) => {
                let is_new = !hash.contains_key(field);
                hash.insert(field.to_string(), value);
                Ok(is_new)
            }
            other => Err(WrongTypeError {
                expected: "hash",
                actual: other.type_name(),
            }),
        }
    }

    /// HGET -- get the value of a field in the hash at `key`.
    pub fn hget(&self, key: &str, field: &str) -> Result<Option<Value>, WrongTypeError> {
        let shard = self.shard(key);
        let data = shard.data.read();
        match data.get(key) {
            Some(KvCollection::Hash(hash)) => Ok(hash.get(field).cloned()),
            Some(other) => Err(WrongTypeError {
                expected: "hash",
                actual: other.type_name(),
            }),
            None => Ok(None),
        }
    }

    /// HDEL -- delete a field from the hash at `key`. Returns true if it existed.
    pub fn hdel(&self, key: &str, field: &str) -> Result<bool, WrongTypeError> {
        #[cfg(feature = "server")]
        if let Some(ref wal) = self.wal
            && let Err(e) = wal.log_hdel(key, field) {
                tracing::error!(target: "nucleus::kv::wal", "WAL write failed: {e}");
            }
        let shard = self.shard(key);
        let mut data = shard.data.write();
        match data.get_mut(key) {
            Some(KvCollection::Hash(hash)) => {
                let removed = hash.remove(field).is_some();
                if hash.is_empty() {
                    data.remove(key);
                }
                Ok(removed)
            }
            Some(other) => Err(WrongTypeError {
                expected: "hash",
                actual: other.type_name(),
            }),
            None => Ok(false),
        }
    }

    /// HGETALL -- return all (field, value) pairs in the hash at `key`.
    pub fn hgetall(&self, key: &str) -> Result<Vec<(String, Value)>, WrongTypeError> {
        let shard = self.shard(key);
        let data = shard.data.read();
        match data.get(key) {
            Some(KvCollection::Hash(hash)) => {
                let mut pairs: Vec<_> = hash
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                pairs.sort_by(|a, b| a.0.cmp(&b.0));
                Ok(pairs)
            }
            Some(other) => Err(WrongTypeError {
                expected: "hash",
                actual: other.type_name(),
            }),
            None => Ok(vec![]),
        }
    }

    /// HKEYS -- return all field names in the hash at `key`.
    pub fn hkeys(&self, key: &str) -> Result<Vec<String>, WrongTypeError> {
        let shard = self.shard(key);
        let data = shard.data.read();
        match data.get(key) {
            Some(KvCollection::Hash(hash)) => {
                let mut keys: Vec<_> = hash.keys().cloned().collect();
                keys.sort();
                Ok(keys)
            }
            Some(other) => Err(WrongTypeError {
                expected: "hash",
                actual: other.type_name(),
            }),
            None => Ok(vec![]),
        }
    }

    /// HVALS -- return all values in the hash at `key`.
    /// Values are returned in field-name-sorted order for determinism.
    pub fn hvals(&self, key: &str) -> Result<Vec<Value>, WrongTypeError> {
        let shard = self.shard(key);
        let data = shard.data.read();
        match data.get(key) {
            Some(KvCollection::Hash(hash)) => {
                let mut pairs: Vec<_> = hash.iter().collect();
                pairs.sort_by_key(|(k, _)| (*k).clone());
                Ok(pairs.into_iter().map(|(_, v)| v.clone()).collect())
            }
            Some(other) => Err(WrongTypeError {
                expected: "hash",
                actual: other.type_name(),
            }),
            None => Ok(vec![]),
        }
    }

    /// HEXISTS -- check if a field exists in the hash at `key`.
    pub fn hexists(&self, key: &str, field: &str) -> Result<bool, WrongTypeError> {
        let shard = self.shard(key);
        let data = shard.data.read();
        match data.get(key) {
            Some(KvCollection::Hash(hash)) => Ok(hash.contains_key(field)),
            Some(other) => Err(WrongTypeError {
                expected: "hash",
                actual: other.type_name(),
            }),
            None => Ok(false),
        }
    }

    /// HLEN -- return the number of fields in the hash at `key`.
    pub fn hlen(&self, key: &str) -> Result<usize, WrongTypeError> {
        let shard = self.shard(key);
        let data = shard.data.read();
        match data.get(key) {
            Some(KvCollection::Hash(hash)) => Ok(hash.len()),
            Some(other) => Err(WrongTypeError {
                expected: "hash",
                actual: other.type_name(),
            }),
            None => Ok(0),
        }
    }

    // ========================================================================
    // Set operations
    // ========================================================================

    /// SADD -- add a member to the set at `key`. Returns true if the member is new.
    pub fn sadd(&self, key: &str, member: &str) -> Result<bool, WrongTypeError> {
        #[cfg(feature = "server")]
        if let Some(ref wal) = self.wal
            && let Err(e) = wal.log_sadd(key, member) {
                tracing::error!(target: "nucleus::kv::wal", "WAL write failed: {e}");
            }
        let shard = self.shard(key);
        let mut data = shard.data.write();
        let entry = data
            .entry(key.to_string())
            .or_insert_with(|| KvCollection::Set(HashSet::new()));
        match entry {
            KvCollection::Set(set) => Ok(set.insert(member.to_string())),
            other => Err(WrongTypeError {
                expected: "set",
                actual: other.type_name(),
            }),
        }
    }

    /// SREM -- remove a member from the set at `key`. Returns true if it existed.
    pub fn srem(&self, key: &str, member: &str) -> Result<bool, WrongTypeError> {
        #[cfg(feature = "server")]
        if let Some(ref wal) = self.wal
            && let Err(e) = wal.log_srem(key, member) {
                tracing::error!(target: "nucleus::kv::wal", "WAL write failed: {e}");
            }
        let shard = self.shard(key);
        let mut data = shard.data.write();
        match data.get_mut(key) {
            Some(KvCollection::Set(set)) => {
                let removed = set.remove(member);
                if set.is_empty() {
                    data.remove(key);
                }
                Ok(removed)
            }
            Some(other) => Err(WrongTypeError {
                expected: "set",
                actual: other.type_name(),
            }),
            None => Ok(false),
        }
    }

    /// SMEMBERS -- return all members of the set at `key`.
    pub fn smembers(&self, key: &str) -> Result<Vec<String>, WrongTypeError> {
        let shard = self.shard(key);
        let data = shard.data.read();
        match data.get(key) {
            Some(KvCollection::Set(set)) => {
                let mut members: Vec<_> = set.iter().cloned().collect();
                members.sort();
                Ok(members)
            }
            Some(other) => Err(WrongTypeError {
                expected: "set",
                actual: other.type_name(),
            }),
            None => Ok(vec![]),
        }
    }

    /// SISMEMBER -- check if `member` is in the set at `key`.
    pub fn sismember(&self, key: &str, member: &str) -> Result<bool, WrongTypeError> {
        let shard = self.shard(key);
        let data = shard.data.read();
        match data.get(key) {
            Some(KvCollection::Set(set)) => Ok(set.contains(member)),
            Some(other) => Err(WrongTypeError {
                expected: "set",
                actual: other.type_name(),
            }),
            None => Ok(false),
        }
    }

    /// SCARD -- return the cardinality (size) of the set at `key`.
    pub fn scard(&self, key: &str) -> Result<usize, WrongTypeError> {
        let shard = self.shard(key);
        let data = shard.data.read();
        match data.get(key) {
            Some(KvCollection::Set(set)) => Ok(set.len()),
            Some(other) => Err(WrongTypeError {
                expected: "set",
                actual: other.type_name(),
            }),
            None => Ok(0),
        }
    }

    /// SINTER -- return the intersection of sets at the given keys.
    /// If any key does not exist, the result is empty.
    /// If any key holds the wrong type, returns an error.
    pub fn sinter(&self, keys: &[&str]) -> Result<Vec<String>, WrongTypeError> {
        if keys.is_empty() {
            return Ok(vec![]);
        }

        // Collect all sets, reading from each shard independently.
        let mut sets: Vec<HashSet<String>> = Vec::with_capacity(keys.len());
        for &key in keys {
            let shard = self.shard(key);
            let data = shard.data.read();
            match data.get(key) {
                Some(KvCollection::Set(set)) => sets.push(set.clone()),
                Some(other) => {
                    return Err(WrongTypeError {
                        expected: "set",
                        actual: other.type_name(),
                    })
                }
                None => return Ok(vec![]), // Empty set intersected = empty
            }
        }

        // Start from the smallest set for efficiency.
        let (min_idx, _) = sets
            .iter()
            .enumerate()
            .min_by_key(|(_, s)| s.len())
            .unwrap();
        let base = sets.swap_remove(min_idx);
        let mut result: Vec<String> = base
            .into_iter()
            .filter(|member| sets.iter().all(|s| s.contains(member)))
            .collect();
        result.sort();
        Ok(result)
    }

    /// SUNION -- return the union of sets at the given keys.
    /// Non-existent keys are treated as empty sets.
    pub fn sunion(&self, keys: &[&str]) -> Result<Vec<String>, WrongTypeError> {
        let mut union = HashSet::new();
        for &key in keys {
            let shard = self.shard(key);
            let data = shard.data.read();
            match data.get(key) {
                Some(KvCollection::Set(set)) => {
                    for member in set {
                        union.insert(member.clone());
                    }
                }
                Some(other) => {
                    return Err(WrongTypeError {
                        expected: "set",
                        actual: other.type_name(),
                    })
                }
                None => {} // Treat as empty
            }
        }
        let mut result: Vec<String> = union.into_iter().collect();
        result.sort();
        Ok(result)
    }

    /// SDIFF -- return the difference: members of the first set minus all other sets.
    /// Non-existent keys (after the first) are treated as empty sets.
    pub fn sdiff(&self, keys: &[&str]) -> Result<Vec<String>, WrongTypeError> {
        if keys.is_empty() {
            return Ok(vec![]);
        }

        // Get the first set.
        let first_key = keys[0];
        let first_set = {
            let shard = self.shard(first_key);
            let data = shard.data.read();
            match data.get(first_key) {
                Some(KvCollection::Set(set)) => set.clone(),
                Some(other) => {
                    return Err(WrongTypeError {
                        expected: "set",
                        actual: other.type_name(),
                    })
                }
                None => return Ok(vec![]),
            }
        };

        // Collect members to subtract.
        let mut subtract = HashSet::new();
        for &key in &keys[1..] {
            let shard = self.shard(key);
            let data = shard.data.read();
            match data.get(key) {
                Some(KvCollection::Set(set)) => {
                    for member in set {
                        subtract.insert(member.clone());
                    }
                }
                Some(other) => {
                    return Err(WrongTypeError {
                        expected: "set",
                        actual: other.type_name(),
                    })
                }
                None => {}
            }
        }

        let mut result: Vec<String> = first_set
            .into_iter()
            .filter(|m| !subtract.contains(m))
            .collect();
        result.sort();
        Ok(result)
    }

    // ========================================================================
    // Sorted Set operations (delegate to SortedSet)
    // ========================================================================

    /// ZADD -- add a member with a score to the sorted set at `key`.
    /// Returns true if the member is new.
    pub fn zadd(
        &self,
        key: &str,
        member: &str,
        score: f64,
    ) -> Result<bool, WrongTypeError> {
        #[cfg(feature = "server")]
        if let Some(ref wal) = self.wal
            && let Err(e) = wal.log_zadd(key, member, score) {
                tracing::error!(target: "nucleus::kv::wal", "WAL write failed: {e}");
            }
        let shard = self.shard(key);
        let mut data = shard.data.write();
        let entry = data
            .entry(key.to_string())
            .or_insert_with(|| KvCollection::SortedSet(SortedSet::new()));
        match entry {
            KvCollection::SortedSet(zset) => Ok(zset.zadd(member, score)),
            other => Err(WrongTypeError {
                expected: "zset",
                actual: other.type_name(),
            }),
        }
    }

    /// ZREM -- remove a member from the sorted set at `key`.
    /// Returns true if the member existed.
    pub fn zrem(&self, key: &str, member: &str) -> Result<bool, WrongTypeError> {
        #[cfg(feature = "server")]
        if let Some(ref wal) = self.wal
            && let Err(e) = wal.log_zrem(key, member) {
                tracing::error!(target: "nucleus::kv::wal", "WAL write failed: {e}");
            }
        let shard = self.shard(key);
        let mut data = shard.data.write();
        match data.get_mut(key) {
            Some(KvCollection::SortedSet(zset)) => {
                let removed = zset.zrem(member);
                if zset.zcard() == 0 {
                    data.remove(key);
                }
                Ok(removed)
            }
            Some(other) => Err(WrongTypeError {
                expected: "zset",
                actual: other.type_name(),
            }),
            None => Ok(false),
        }
    }

    /// ZRANGE -- return entries by rank ascending, from `start` to `stop` (inclusive).
    pub fn zrange(
        &self,
        key: &str,
        start: usize,
        stop: usize,
    ) -> Result<Vec<SortedSetEntry>, WrongTypeError> {
        let shard = self.shard(key);
        let data = shard.data.read();
        match data.get(key) {
            Some(KvCollection::SortedSet(zset)) => Ok(zset.zrange(start, stop)),
            Some(other) => Err(WrongTypeError {
                expected: "zset",
                actual: other.type_name(),
            }),
            None => Ok(vec![]),
        }
    }

    /// ZREVRANGE -- return entries by rank descending, from `start` to `stop` (inclusive).
    pub fn zrevrange(
        &self,
        key: &str,
        start: usize,
        stop: usize,
    ) -> Result<Vec<SortedSetEntry>, WrongTypeError> {
        let shard = self.shard(key);
        let data = shard.data.read();
        match data.get(key) {
            Some(KvCollection::SortedSet(zset)) => Ok(zset.zrevrange(start, stop)),
            Some(other) => Err(WrongTypeError {
                expected: "zset",
                actual: other.type_name(),
            }),
            None => Ok(vec![]),
        }
    }

    /// ZRANGEBYSCORE -- return entries with scores in [min, max].
    pub fn zrangebyscore(
        &self,
        key: &str,
        min: f64,
        max: f64,
    ) -> Result<Vec<SortedSetEntry>, WrongTypeError> {
        let shard = self.shard(key);
        let data = shard.data.read();
        match data.get(key) {
            Some(KvCollection::SortedSet(zset)) => Ok(zset.zrangebyscore(min, max)),
            Some(other) => Err(WrongTypeError {
                expected: "zset",
                actual: other.type_name(),
            }),
            None => Ok(vec![]),
        }
    }

    /// ZRANK -- return the 0-based rank of `member` by ascending score.
    pub fn zrank(
        &self,
        key: &str,
        member: &str,
    ) -> Result<Option<usize>, WrongTypeError> {
        let shard = self.shard(key);
        let data = shard.data.read();
        match data.get(key) {
            Some(KvCollection::SortedSet(zset)) => Ok(zset.zrank(member)),
            Some(other) => Err(WrongTypeError {
                expected: "zset",
                actual: other.type_name(),
            }),
            None => Ok(None),
        }
    }

    /// ZINCRBY -- increment the score of `member` by `increment`.
    /// Creates the member with `increment` as the score if it does not exist.
    /// Returns the new score.
    pub fn zincrby(
        &self,
        key: &str,
        member: &str,
        increment: f64,
    ) -> Result<f64, WrongTypeError> {
        #[cfg(feature = "server")]
        if let Some(ref wal) = self.wal
            && let Err(e) = wal.log_zincrby(key, member, increment) {
                tracing::error!(target: "nucleus::kv::wal", "WAL write failed: {e}");
            }
        let shard = self.shard(key);
        let mut data = shard.data.write();
        let entry = data
            .entry(key.to_string())
            .or_insert_with(|| KvCollection::SortedSet(SortedSet::new()));
        match entry {
            KvCollection::SortedSet(zset) => Ok(zset.zincrby(member, increment)),
            other => Err(WrongTypeError {
                expected: "zset",
                actual: other.type_name(),
            }),
        }
    }

    /// ZCARD -- return the number of members in the sorted set at `key`.
    pub fn zcard(&self, key: &str) -> Result<usize, WrongTypeError> {
        let shard = self.shard(key);
        let data = shard.data.read();
        match data.get(key) {
            Some(KvCollection::SortedSet(zset)) => Ok(zset.zcard()),
            Some(other) => Err(WrongTypeError {
                expected: "zset",
                actual: other.type_name(),
            }),
            None => Ok(0),
        }
    }

    /// ZCOUNT -- count entries with scores in [min, max].
    pub fn zcount(
        &self,
        key: &str,
        min: f64,
        max: f64,
    ) -> Result<usize, WrongTypeError> {
        let shard = self.shard(key);
        let data = shard.data.read();
        match data.get(key) {
            Some(KvCollection::SortedSet(zset)) => Ok(zset.zcount(min, max)),
            Some(other) => Err(WrongTypeError {
                expected: "zset",
                actual: other.type_name(),
            }),
            None => Ok(0),
        }
    }

    // ========================================================================
    // HyperLogLog operations (delegate to HyperLogLog)
    // ========================================================================

    /// PFADD -- add an element to the HyperLogLog at `key`.
    /// Returns true if the internal registers changed (cardinality may have changed).
    pub fn pfadd(&self, key: &str, element: &str) -> Result<bool, WrongTypeError> {
        #[cfg(feature = "server")]
        if let Some(ref wal) = self.wal
            && let Err(e) = wal.log_pfadd(key, element) {
                tracing::error!(target: "nucleus::kv::wal", "WAL write failed: {e}");
            }
        let shard = self.shard(key);
        let mut data = shard.data.write();
        let entry = data
            .entry(key.to_string())
            .or_insert_with(|| KvCollection::HyperLogLog(HyperLogLog::new()));
        match entry {
            KvCollection::HyperLogLog(hll) => {
                let before = hll.count();
                hll.add(element);
                let after = hll.count();
                Ok(after != before)
            }
            other => Err(WrongTypeError {
                expected: "hyperloglog",
                actual: other.type_name(),
            }),
        }
    }

    /// PFCOUNT -- return the estimated cardinality of the HyperLogLog at `key`.
    pub fn pfcount(&self, key: &str) -> Result<u64, WrongTypeError> {
        let shard = self.shard(key);
        let data = shard.data.read();
        match data.get(key) {
            Some(KvCollection::HyperLogLog(hll)) => Ok(hll.count()),
            Some(other) => Err(WrongTypeError {
                expected: "hyperloglog",
                actual: other.type_name(),
            }),
            None => Ok(0),
        }
    }

    /// PFMERGE -- merge HyperLogLogs from `source_keys` into `dest_key`.
    /// Creates `dest_key` if it does not exist. Source HLLs that do not exist
    /// are silently skipped.
    pub fn pfmerge(
        &self,
        dest_key: &str,
        source_keys: &[&str],
    ) -> Result<(), WrongTypeError> {
        // First, collect register snapshots from all sources.
        let mut source_registers: Vec<Vec<u8>> = Vec::new();
        for &src_key in source_keys {
            let shard = self.shard(src_key);
            let data = shard.data.read();
            match data.get(src_key) {
                Some(KvCollection::HyperLogLog(hll)) => {
                    // Snapshot the registers since we cannot hold multiple shard locks.
                    source_registers.push(hll.registers().to_vec());
                }
                Some(other) => {
                    return Err(WrongTypeError {
                        expected: "hyperloglog",
                        actual: other.type_name(),
                    })
                }
                None => {} // Skip non-existent keys
            }
        }

        // Log the merge to WAL with the register snapshots
        #[cfg(feature = "server")]
        if let Some(ref wal) = self.wal
            && let Err(e) = wal.log_pfmerge(dest_key, &source_registers) {
                tracing::error!(target: "nucleus::kv::wal", "WAL write failed: {e}");
            }

        // Now write-lock the destination and merge.
        let dest_shard = self.shard(dest_key);
        let mut data = dest_shard.data.write();
        let entry = data
            .entry(dest_key.to_string())
            .or_insert_with(|| KvCollection::HyperLogLog(HyperLogLog::new()));
        match entry {
            KvCollection::HyperLogLog(dest_hll) => {
                for src_regs in &source_registers {
                    let mut src_hll = HyperLogLog::new();
                    src_hll.set_registers(src_regs);
                    dest_hll.merge(&src_hll);
                }
                Ok(())
            }
            other => Err(WrongTypeError {
                expected: "hyperloglog",
                actual: other.type_name(),
            }),
        }
    }

    // ========================================================================
    // Utility
    // ========================================================================

    /// Remove a key regardless of its collection type. Returns true if the key existed.
    pub fn del(&self, key: &str) -> bool {
        #[cfg(feature = "server")]
        if let Some(ref wal) = self.wal
            && let Err(e) = wal.log_del(key) {
                tracing::error!(target: "nucleus::kv::wal", "WAL write failed: {e}");
            }
        let shard = self.shard(key);
        shard.data.write().remove(key).is_some()
    }

    /// Check if a key exists (any collection type).
    pub fn exists(&self, key: &str) -> bool {
        let shard = self.shard(key);
        shard.data.read().contains_key(key)
    }

    /// Return the type name of the collection at `key`, or None if the key does not exist.
    pub fn key_type(&self, key: &str) -> Option<&'static str> {
        let shard = self.shard(key);
        let data = shard.data.read();
        data.get(key).map(|c| c.type_name())
    }

    // ========================================================================
    // WAL support helpers
    // ========================================================================

    /// Snapshot all collections across all shards. Used by WAL checkpoint.
    pub fn snapshot_all(&self) -> Vec<(String, KvCollection)> {
        let mut result = Vec::new();
        for shard in &self.shards {
            let data = shard.data.read();
            for (key, coll) in data.iter() {
                result.push((key.clone(), clone_collection(coll)));
            }
        }
        result
    }

    /// Clear all collections across all shards. Used by WAL snapshot replay.
    pub fn clear_all(&self) {
        for shard in &self.shards {
            shard.data.write().clear();
        }
    }

    /// Insert a collection directly into the appropriate shard. Used by WAL replay.
    pub fn insert_collection(&self, key: &str, coll: KvCollection) {
        let shard = self.shard(key);
        shard.data.write().insert(key.to_string(), coll);
    }

    /// Merge HLL registers into the destination key. Used by WAL pfmerge replay.
    pub fn pfmerge_registers(&self, dest_key: &str, regs: &[u8]) {
        let shard = self.shard(dest_key);
        let mut data = shard.data.write();
        let entry = data
            .entry(dest_key.to_string())
            .or_insert_with(|| KvCollection::HyperLogLog(HyperLogLog::new()));
        if let KvCollection::HyperLogLog(dest_hll) = entry {
            let mut src_hll = HyperLogLog::new();
            src_hll.set_registers(regs);
            dest_hll.merge(&src_hll);
        }
    }

    /// Get HLL register snapshots for the given keys. Non-existent keys are skipped.
    pub fn get_hll_registers(&self, keys: &[&str]) -> Vec<Vec<u8>> {
        let mut result = Vec::new();
        for &key in keys {
            let shard = self.shard(key);
            let data = shard.data.read();
            if let Some(KvCollection::HyperLogLog(hll)) = data.get(key) {
                result.push(hll.registers().to_vec());
            }
        }
        result
    }

    /// Write a WAL checkpoint. No-op if WAL is disabled.
    #[cfg(feature = "server")]
    pub fn checkpoint(&self) -> std::io::Result<()> {
        let Some(ref wal) = self.wal else {
            return Ok(());
        };
        wal.checkpoint(self)
    }

    /// No-op checkpoint when WAL is not available.
    #[cfg(not(feature = "server"))]
    pub fn checkpoint(&self) -> std::io::Result<()> { Ok(()) }

    // ====================================================================
    // Stream operations
    // ====================================================================

    /// XADD: add an entry to a stream. Auto-creates the stream if needed.
    pub fn xadd(
        &self,
        key: &str,
        id_str: &str,
        fields: Vec<(String, String)>,
    ) -> Result<super::streams::StreamId, String> {
        let shard = self.shard(key);
        let mut data = shard.data.write();
        let stream = data
            .entry(key.to_string())
            .or_insert_with(|| KvCollection::Stream(Stream::new()));
        match stream {
            KvCollection::Stream(s) => s.xadd(id_str, fields),
            other => Err(format!(
                "WRONGTYPE Operation against a key holding the wrong kind of value (expected stream, got {})",
                other.type_name()
            )),
        }
    }

    /// XLEN: get the number of entries in a stream.
    pub fn xlen(&self, key: &str) -> Result<usize, WrongTypeError> {
        let shard = self.shard(key);
        let data = shard.data.read();
        match data.get(key) {
            Some(KvCollection::Stream(s)) => Ok(s.xlen()),
            Some(other) => Err(WrongTypeError {
                expected: "stream",
                actual: other.type_name(),
            }),
            None => Ok(0),
        }
    }

    /// XRANGE: get entries in a range.
    pub fn xrange(
        &self,
        key: &str,
        start: &str,
        end: &str,
        count: Option<usize>,
    ) -> Result<Vec<super::streams::StreamEntry>, WrongTypeError> {
        let shard = self.shard(key);
        let data = shard.data.read();
        match data.get(key) {
            Some(KvCollection::Stream(s)) => {
                Ok(s.xrange(start, end, count).into_iter().cloned().collect())
            }
            Some(other) => Err(WrongTypeError {
                expected: "stream",
                actual: other.type_name(),
            }),
            None => Ok(Vec::new()),
        }
    }

    /// XREVRANGE: get entries in reverse order.
    pub fn xrevrange(
        &self,
        key: &str,
        end: &str,
        start: &str,
        count: Option<usize>,
    ) -> Result<Vec<super::streams::StreamEntry>, WrongTypeError> {
        let shard = self.shard(key);
        let data = shard.data.read();
        match data.get(key) {
            Some(KvCollection::Stream(s)) => {
                Ok(s.xrevrange(end, start, count).into_iter().cloned().collect())
            }
            Some(other) => Err(WrongTypeError {
                expected: "stream",
                actual: other.type_name(),
            }),
            None => Ok(Vec::new()),
        }
    }

    /// XREAD: read entries after a given ID.
    pub fn xread(
        &self,
        key: &str,
        last_id: &str,
        count: Option<usize>,
    ) -> Result<Vec<super::streams::StreamEntry>, WrongTypeError> {
        let shard = self.shard(key);
        let data = shard.data.read();
        match data.get(key) {
            Some(KvCollection::Stream(s)) => {
                Ok(s.xread(last_id, count).into_iter().cloned().collect())
            }
            Some(other) => Err(WrongTypeError {
                expected: "stream",
                actual: other.type_name(),
            }),
            None => Ok(Vec::new()),
        }
    }

    /// XDEL: delete entries from a stream.
    pub fn xdel(
        &self,
        key: &str,
        ids: &[super::streams::StreamId],
    ) -> Result<usize, WrongTypeError> {
        let shard = self.shard(key);
        let mut data = shard.data.write();
        match data.get_mut(key) {
            Some(KvCollection::Stream(s)) => Ok(s.xdel(ids)),
            Some(other) => Err(WrongTypeError {
                expected: "stream",
                actual: other.type_name(),
            }),
            None => Ok(0),
        }
    }

    /// XTRIM MAXLEN: trim stream to at most maxlen entries.
    pub fn xtrim_maxlen(&self, key: &str, maxlen: usize) -> Result<usize, WrongTypeError> {
        let shard = self.shard(key);
        let mut data = shard.data.write();
        match data.get_mut(key) {
            Some(KvCollection::Stream(s)) => Ok(s.xtrim_maxlen(maxlen)),
            Some(other) => Err(WrongTypeError {
                expected: "stream",
                actual: other.type_name(),
            }),
            None => Ok(0),
        }
    }

    /// XGROUP CREATE: create a consumer group.
    pub fn xgroup_create(
        &self,
        key: &str,
        group_name: &str,
        start_id: &str,
    ) -> Result<(), String> {
        let shard = self.shard(key);
        let mut data = shard.data.write();
        let stream = data
            .entry(key.to_string())
            .or_insert_with(|| KvCollection::Stream(Stream::new()));
        match stream {
            KvCollection::Stream(s) => s.xgroup_create(group_name, start_id),
            other => Err(format!(
                "WRONGTYPE expected stream, got {}",
                other.type_name()
            )),
        }
    }

    /// XGROUP DESTROY: destroy a consumer group.
    pub fn xgroup_destroy(&self, key: &str, group_name: &str) -> Result<bool, WrongTypeError> {
        let shard = self.shard(key);
        let mut data = shard.data.write();
        match data.get_mut(key) {
            Some(KvCollection::Stream(s)) => Ok(s.xgroup_destroy(group_name)),
            Some(other) => Err(WrongTypeError {
                expected: "stream",
                actual: other.type_name(),
            }),
            None => Ok(false),
        }
    }

    /// XREADGROUP: read entries for a consumer group member.
    pub fn xreadgroup(
        &self,
        key: &str,
        group_name: &str,
        consumer_name: &str,
        pending_id: &str,
        count: Option<usize>,
    ) -> Result<Vec<super::streams::StreamEntry>, String> {
        let shard = self.shard(key);
        let mut data = shard.data.write();
        match data.get_mut(key) {
            Some(KvCollection::Stream(s)) => {
                s.xreadgroup(group_name, consumer_name, pending_id, count)
            }
            Some(other) => Err(format!(
                "WRONGTYPE expected stream, got {}",
                other.type_name()
            )),
            None => Err("ERR no such key".to_string()),
        }
    }

    /// XACK: acknowledge entries in a consumer group.
    pub fn xack(
        &self,
        key: &str,
        group_name: &str,
        ids: &[super::streams::StreamId],
    ) -> Result<usize, String> {
        let shard = self.shard(key);
        let mut data = shard.data.write();
        match data.get_mut(key) {
            Some(KvCollection::Stream(s)) => s.xack(group_name, ids),
            Some(other) => Err(format!(
                "WRONGTYPE expected stream, got {}",
                other.type_name()
            )),
            None => Ok(0),
        }
    }

    // ====================================================================
    // Geo operations
    // ====================================================================

    /// GEOADD: add a member with (longitude, latitude) to a geo set.
    /// Returns true if the member is new.
    pub fn geoadd(
        &self,
        key: &str,
        lon: f64,
        lat: f64,
        member: &str,
    ) -> Result<bool, WrongTypeError> {
        let shard = self.shard(key);
        let mut data = shard.data.write();
        let entry = data
            .entry(key.to_string())
            .or_insert_with(|| KvCollection::Geo(GeoSet::new()));
        match entry {
            KvCollection::Geo(g) => Ok(g.add(lon, lat, member)),
            other => Err(WrongTypeError {
                expected: "geo",
                actual: other.type_name(),
            }),
        }
    }

    /// GEOPOS: get the position of a member. Returns (lon, lat) or None.
    pub fn geopos(&self, key: &str, member: &str) -> Result<Option<(f64, f64)>, WrongTypeError> {
        let shard = self.shard(key);
        let data = shard.data.read();
        match data.get(key) {
            Some(KvCollection::Geo(g)) => Ok(g.pos(member)),
            Some(other) => Err(WrongTypeError {
                expected: "geo",
                actual: other.type_name(),
            }),
            None => Ok(None),
        }
    }

    /// GEODIST: compute distance between two members.
    pub fn geodist(
        &self,
        key: &str,
        member1: &str,
        member2: &str,
        unit: &str,
    ) -> Result<Option<f64>, WrongTypeError> {
        let shard = self.shard(key);
        let data = shard.data.read();
        match data.get(key) {
            Some(KvCollection::Geo(g)) => Ok(g.dist(member1, member2, unit)),
            Some(other) => Err(WrongTypeError {
                expected: "geo",
                actual: other.type_name(),
            }),
            None => Ok(None),
        }
    }

    /// GEORADIUS: find members within a radius.
    pub fn georadius(
        &self,
        key: &str,
        lon: f64,
        lat: f64,
        radius: f64,
        unit: &str,
    ) -> Result<Vec<(String, f64)>, WrongTypeError> {
        let shard = self.shard(key);
        let data = shard.data.read();
        match data.get(key) {
            Some(KvCollection::Geo(g)) => Ok(g.radius(lon, lat, radius, unit)),
            Some(other) => Err(WrongTypeError {
                expected: "geo",
                actual: other.type_name(),
            }),
            None => Ok(Vec::new()),
        }
    }

    /// GEOLEN: number of members in a geo set.
    pub fn geolen(&self, key: &str) -> Result<usize, WrongTypeError> {
        let shard = self.shard(key);
        let data = shard.data.read();
        match data.get(key) {
            Some(KvCollection::Geo(g)) => Ok(g.len()),
            Some(other) => Err(WrongTypeError {
                expected: "geo",
                actual: other.type_name(),
            }),
            None => Ok(0),
        }
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Deep-clone a KvCollection (needed for snapshot serialization).
fn clone_collection(coll: &KvCollection) -> KvCollection {
    match coll {
        KvCollection::List(list) => KvCollection::List(list.clone()),
        KvCollection::Hash(hash) => KvCollection::Hash(hash.clone()),
        KvCollection::Set(set) => KvCollection::Set(set.clone()),
        KvCollection::SortedSet(zset) => {
            // Rebuild from entries since SortedSet fields are private
            let mut new_zset = SortedSet::new();
            let entries = zset.zrange(0, zset.zcard().saturating_sub(1));
            for entry in &entries {
                new_zset.zadd(&entry.member, entry.score);
            }
            KvCollection::SortedSet(new_zset)
        }
        KvCollection::HyperLogLog(hll) => {
            let mut new_hll = HyperLogLog::new();
            new_hll.set_registers(hll.registers());
            KvCollection::HyperLogLog(new_hll)
        }
        KvCollection::Stream(s) => KvCollection::Stream(s.clone()),
        KvCollection::Geo(g) => KvCollection::Geo(g.clone()),
    }
}

/// Normalize a possibly-negative index into a `usize` offset.
/// Negative values count from the end: -1 = len-1, -2 = len-2, etc.
/// Out-of-range negatives clamp to 0.
fn normalize_index(index: i64, len: i64) -> usize {
    if index < 0 {
        let adjusted = len + index;
        if adjusted < 0 { 0 } else { adjusted as usize }
    } else {
        index as usize
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value;

    // ------------------------------------------------------------------------
    // List tests
    // ------------------------------------------------------------------------

    #[test]
    fn list_lpush_rpush() {
        let c = ShardedCollections::new();
        assert_eq!(c.lpush("mylist", Value::Text("a".into())).unwrap(), 1);
        assert_eq!(c.lpush("mylist", Value::Text("b".into())).unwrap(), 2);
        assert_eq!(c.rpush("mylist", Value::Text("c".into())).unwrap(), 3);
        // List should be: b, a, c
        let items = c.lrange("mylist", 0, -1).unwrap();
        assert_eq!(
            items,
            vec![
                Value::Text("b".into()),
                Value::Text("a".into()),
                Value::Text("c".into()),
            ]
        );
    }

    #[test]
    fn list_lpop_rpop() {
        let c = ShardedCollections::new();
        c.rpush("q", Value::Int32(1)).unwrap();
        c.rpush("q", Value::Int32(2)).unwrap();
        c.rpush("q", Value::Int32(3)).unwrap();

        assert_eq!(c.lpop("q").unwrap(), Some(Value::Int32(1)));
        assert_eq!(c.rpop("q").unwrap(), Some(Value::Int32(3)));
        assert_eq!(c.llen("q").unwrap(), 1);

        assert_eq!(c.lpop("q").unwrap(), Some(Value::Int32(2)));
        assert!(!c.exists("q"));
        assert_eq!(c.lpop("q").unwrap(), None);
    }

    #[test]
    fn list_lrange_negative_indices() {
        let c = ShardedCollections::new();
        for i in 0..5 {
            c.rpush("nums", Value::Int32(i)).unwrap();
        }
        assert_eq!(
            c.lrange("nums", -2, -1).unwrap(),
            vec![Value::Int32(3), Value::Int32(4)]
        );
        assert_eq!(
            c.lrange("nums", 1, 3).unwrap(),
            vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)]
        );
        assert_eq!(c.lrange("nums", 10, 20).unwrap(), Vec::<Value>::new());
    }

    #[test]
    fn list_lrange_on_nonexistent_key() {
        let c = ShardedCollections::new();
        assert_eq!(c.lrange("missing", 0, -1).unwrap(), Vec::<Value>::new());
    }

    #[test]
    fn list_lindex() {
        let c = ShardedCollections::new();
        c.rpush("idx", Value::Text("zero".into())).unwrap();
        c.rpush("idx", Value::Text("one".into())).unwrap();
        c.rpush("idx", Value::Text("two".into())).unwrap();

        assert_eq!(c.lindex("idx", 0).unwrap(), Some(Value::Text("zero".into())));
        assert_eq!(c.lindex("idx", -1).unwrap(), Some(Value::Text("two".into())));
        assert_eq!(c.lindex("idx", -3).unwrap(), Some(Value::Text("zero".into())));
        assert_eq!(c.lindex("idx", 5).unwrap(), None);
    }

    #[test]
    fn list_llen_empty() {
        let c = ShardedCollections::new();
        assert_eq!(c.llen("nope").unwrap(), 0);
    }

    #[test]
    fn list_wrong_type() {
        let c = ShardedCollections::new();
        c.sadd("myset", "a").unwrap();
        let err = c.lpush("myset", Value::Int32(1)).unwrap_err();
        assert_eq!(err.expected, "list");
        assert_eq!(err.actual, "set");
    }

    // ------------------------------------------------------------------------
    // Hash tests
    // ------------------------------------------------------------------------

    #[test]
    fn hash_hset_hget() {
        let c = ShardedCollections::new();
        assert!(c.hset("user:1", "name", Value::Text("Alice".into())).unwrap());
        assert!(!c.hset("user:1", "name", Value::Text("Bob".into())).unwrap());
        assert_eq!(
            c.hget("user:1", "name").unwrap(),
            Some(Value::Text("Bob".into()))
        );
        assert_eq!(c.hget("user:1", "missing").unwrap(), None);
    }

    #[test]
    fn hash_hdel() {
        let c = ShardedCollections::new();
        c.hset("h", "f1", Value::Int32(1)).unwrap();
        c.hset("h", "f2", Value::Int32(2)).unwrap();
        assert!(c.hdel("h", "f1").unwrap());
        assert!(!c.hdel("h", "f1").unwrap());
        assert_eq!(c.hlen("h").unwrap(), 1);

        c.hdel("h", "f2").unwrap();
        assert!(!c.exists("h"));
    }

    #[test]
    fn hash_hgetall_hkeys_hvals() {
        let c = ShardedCollections::new();
        c.hset("profile", "age", Value::Int32(30)).unwrap();
        c.hset("profile", "city", Value::Text("NYC".into())).unwrap();

        let all = c.hgetall("profile").unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0], ("age".to_string(), Value::Int32(30)));
        assert_eq!(all[1], ("city".to_string(), Value::Text("NYC".into())));

        let keys = c.hkeys("profile").unwrap();
        assert_eq!(keys, vec!["age", "city"]);

        let vals = c.hvals("profile").unwrap();
        assert_eq!(vals, vec![Value::Int32(30), Value::Text("NYC".into())]);
    }

    #[test]
    fn hash_hexists() {
        let c = ShardedCollections::new();
        c.hset("h", "present", Value::Int32(1)).unwrap();
        assert!(c.hexists("h", "present").unwrap());
        assert!(!c.hexists("h", "absent").unwrap());
        assert!(!c.hexists("nonexistent", "field").unwrap());
    }

    #[test]
    fn hash_hlen_empty() {
        let c = ShardedCollections::new();
        assert_eq!(c.hlen("nohash").unwrap(), 0);
    }

    #[test]
    fn hash_hgetall_empty() {
        let c = ShardedCollections::new();
        assert_eq!(c.hgetall("none").unwrap(), Vec::<(String, Value)>::new());
    }

    #[test]
    fn hash_wrong_type() {
        let c = ShardedCollections::new();
        c.rpush("list", Value::Int32(1)).unwrap();
        let err = c.hset("list", "f", Value::Int32(1)).unwrap_err();
        assert_eq!(err.expected, "hash");
        assert_eq!(err.actual, "list");
    }

    // ------------------------------------------------------------------------
    // Set tests
    // ------------------------------------------------------------------------

    #[test]
    fn set_sadd_srem() {
        let c = ShardedCollections::new();
        assert!(c.sadd("tags", "rust").unwrap());
        assert!(c.sadd("tags", "mojo").unwrap());
        assert!(!c.sadd("tags", "rust").unwrap());
        assert_eq!(c.scard("tags").unwrap(), 2);

        assert!(c.srem("tags", "rust").unwrap());
        assert!(!c.srem("tags", "rust").unwrap());
        assert_eq!(c.scard("tags").unwrap(), 1);
    }

    #[test]
    fn set_smembers_sismember() {
        let c = ShardedCollections::new();
        c.sadd("s", "a").unwrap();
        c.sadd("s", "b").unwrap();
        c.sadd("s", "c").unwrap();

        assert!(c.sismember("s", "b").unwrap());
        assert!(!c.sismember("s", "z").unwrap());

        let members = c.smembers("s").unwrap();
        assert_eq!(members, vec!["a", "b", "c"]);
    }

    #[test]
    fn set_scard_empty() {
        let c = ShardedCollections::new();
        assert_eq!(c.scard("noset").unwrap(), 0);
    }

    #[test]
    fn set_auto_remove_on_empty() {
        let c = ShardedCollections::new();
        c.sadd("temp", "x").unwrap();
        c.srem("temp", "x").unwrap();
        assert!(!c.exists("temp"));
    }

    #[test]
    fn set_sinter() {
        let c = ShardedCollections::new();
        c.sadd("s1", "a").unwrap();
        c.sadd("s1", "b").unwrap();
        c.sadd("s1", "c").unwrap();

        c.sadd("s2", "b").unwrap();
        c.sadd("s2", "c").unwrap();
        c.sadd("s2", "d").unwrap();

        c.sadd("s3", "c").unwrap();
        c.sadd("s3", "d").unwrap();
        c.sadd("s3", "e").unwrap();

        let result = c.sinter(&["s1", "s2", "s3"]).unwrap();
        assert_eq!(result, vec!["c"]);
    }

    #[test]
    fn set_sinter_empty_key() {
        let c = ShardedCollections::new();
        c.sadd("s1", "a").unwrap();
        let result = c.sinter(&["s1", "nonexistent"]).unwrap();
        assert_eq!(result, Vec::<String>::new());
    }

    #[test]
    fn set_sunion() {
        let c = ShardedCollections::new();
        c.sadd("u1", "a").unwrap();
        c.sadd("u1", "b").unwrap();

        c.sadd("u2", "b").unwrap();
        c.sadd("u2", "c").unwrap();

        let result = c.sunion(&["u1", "u2"]).unwrap();
        assert_eq!(result, vec!["a", "b", "c"]);
    }

    #[test]
    fn set_sunion_with_missing_key() {
        let c = ShardedCollections::new();
        c.sadd("x", "a").unwrap();
        let result = c.sunion(&["x", "missing"]).unwrap();
        assert_eq!(result, vec!["a"]);
    }

    #[test]
    fn set_sdiff() {
        let c = ShardedCollections::new();
        c.sadd("d1", "a").unwrap();
        c.sadd("d1", "b").unwrap();
        c.sadd("d1", "c").unwrap();

        c.sadd("d2", "b").unwrap();
        c.sadd("d3", "c").unwrap();

        let result = c.sdiff(&["d1", "d2", "d3"]).unwrap();
        assert_eq!(result, vec!["a"]);
    }

    #[test]
    fn set_sdiff_first_missing() {
        let c = ShardedCollections::new();
        c.sadd("present", "a").unwrap();
        let result = c.sdiff(&["missing", "present"]).unwrap();
        assert_eq!(result, Vec::<String>::new());
    }

    #[test]
    fn set_wrong_type() {
        let c = ShardedCollections::new();
        c.hset("h", "f", Value::Int32(1)).unwrap();
        let err = c.sadd("h", "member").unwrap_err();
        assert_eq!(err.expected, "set");
        assert_eq!(err.actual, "hash");
    }

    // ------------------------------------------------------------------------
    // Sorted Set tests
    // ------------------------------------------------------------------------

    #[test]
    fn zset_zadd_zrange() {
        let c = ShardedCollections::new();
        assert!(c.zadd("leaderboard", "alice", 100.0).unwrap());
        assert!(c.zadd("leaderboard", "bob", 200.0).unwrap());
        assert!(c.zadd("leaderboard", "charlie", 150.0).unwrap());

        let entries = c.zrange("leaderboard", 0, 2).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].member, "alice");
        assert_eq!(entries[0].score, 100.0);
        assert_eq!(entries[1].member, "charlie");
        assert_eq!(entries[2].member, "bob");
    }

    #[test]
    fn zset_zadd_update_score() {
        let c = ShardedCollections::new();
        assert!(c.zadd("z", "m", 1.0).unwrap());
        assert!(!c.zadd("z", "m", 5.0).unwrap());
        let entries = c.zrange("z", 0, 0).unwrap();
        assert_eq!(entries[0].score, 5.0);
    }

    #[test]
    fn zset_zrem() {
        let c = ShardedCollections::new();
        c.zadd("z", "a", 1.0).unwrap();
        c.zadd("z", "b", 2.0).unwrap();
        assert!(c.zrem("z", "a").unwrap());
        assert!(!c.zrem("z", "a").unwrap());
        assert_eq!(c.zcard("z").unwrap(), 1);

        c.zrem("z", "b").unwrap();
        assert!(!c.exists("z"));
    }

    #[test]
    fn zset_zrevrange() {
        let c = ShardedCollections::new();
        c.zadd("r", "x", 10.0).unwrap();
        c.zadd("r", "y", 20.0).unwrap();
        c.zadd("r", "z", 30.0).unwrap();

        let entries = c.zrevrange("r", 0, 1).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].member, "z");
        assert_eq!(entries[1].member, "y");
    }

    #[test]
    fn zset_zrangebyscore() {
        let c = ShardedCollections::new();
        c.zadd("scores", "a", 1.0).unwrap();
        c.zadd("scores", "b", 5.0).unwrap();
        c.zadd("scores", "c", 10.0).unwrap();
        c.zadd("scores", "d", 15.0).unwrap();

        let entries = c.zrangebyscore("scores", 5.0, 10.0).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].member, "b");
        assert_eq!(entries[1].member, "c");
    }

    #[test]
    fn zset_zrank() {
        let c = ShardedCollections::new();
        c.zadd("ranking", "first", 1.0).unwrap();
        c.zadd("ranking", "second", 2.0).unwrap();
        c.zadd("ranking", "third", 3.0).unwrap();

        assert_eq!(c.zrank("ranking", "first").unwrap(), Some(0));
        assert_eq!(c.zrank("ranking", "third").unwrap(), Some(2));
        assert_eq!(c.zrank("ranking", "missing").unwrap(), None);
    }

    #[test]
    fn zset_zincrby() {
        let c = ShardedCollections::new();
        let score = c.zincrby("z", "m", 5.0).unwrap();
        assert_eq!(score, 5.0);
        let score = c.zincrby("z", "m", 3.0).unwrap();
        assert_eq!(score, 8.0);
    }

    #[test]
    fn zset_zcard_zcount() {
        let c = ShardedCollections::new();
        assert_eq!(c.zcard("empty").unwrap(), 0);

        c.zadd("z", "a", 1.0).unwrap();
        c.zadd("z", "b", 5.0).unwrap();
        c.zadd("z", "c", 10.0).unwrap();

        assert_eq!(c.zcard("z").unwrap(), 3);
        assert_eq!(c.zcount("z", 1.0, 5.0).unwrap(), 2);
        assert_eq!(c.zcount("z", 100.0, 200.0).unwrap(), 0);
    }

    #[test]
    fn zset_on_nonexistent_key() {
        let c = ShardedCollections::new();
        assert_eq!(c.zrange("nope", 0, 10).unwrap(), vec![]);
        assert_eq!(c.zrevrange("nope", 0, 10).unwrap(), vec![]);
        assert_eq!(c.zrangebyscore("nope", 0.0, 100.0).unwrap(), vec![]);
        assert_eq!(c.zrank("nope", "x").unwrap(), None);
        assert_eq!(c.zcount("nope", 0.0, 100.0).unwrap(), 0);
    }

    #[test]
    fn zset_wrong_type() {
        let c = ShardedCollections::new();
        c.sadd("s", "member").unwrap();
        let err = c.zadd("s", "m", 1.0).unwrap_err();
        assert_eq!(err.expected, "zset");
        assert_eq!(err.actual, "set");
    }

    // ------------------------------------------------------------------------
    // HyperLogLog tests
    // ------------------------------------------------------------------------

    #[test]
    fn hll_pfadd_pfcount() {
        let c = ShardedCollections::new();
        assert!(c.pfadd("visitors", "user1").unwrap());
        assert!(c.pfadd("visitors", "user2").unwrap());
        assert!(c.pfadd("visitors", "user3").unwrap());

        let count = c.pfcount("visitors").unwrap();
        assert!(count >= 2 && count <= 5, "expected ~3, got {count}");
    }

    #[test]
    fn hll_pfadd_duplicate() {
        let c = ShardedCollections::new();
        c.pfadd("hll", "a").unwrap();
        let count_before = c.pfcount("hll").unwrap();
        let changed = c.pfadd("hll", "a").unwrap();
        assert!(!changed);
        assert_eq!(c.pfcount("hll").unwrap(), count_before);
    }

    #[test]
    fn hll_pfcount_nonexistent() {
        let c = ShardedCollections::new();
        assert_eq!(c.pfcount("missing").unwrap(), 0);
    }

    #[test]
    fn hll_pfmerge() {
        let c = ShardedCollections::new();
        for i in 0..50 {
            c.pfadd("hll1", &format!("item{i}")).unwrap();
        }
        for i in 50..100 {
            c.pfadd("hll2", &format!("item{i}")).unwrap();
        }

        c.pfmerge("merged", &["hll1", "hll2"]).unwrap();
        let count = c.pfcount("merged").unwrap();
        assert!(
            count >= 85 && count <= 115,
            "expected ~100, got {count}"
        );
    }

    #[test]
    fn hll_pfmerge_into_existing() {
        let c = ShardedCollections::new();
        for i in 0..20 {
            c.pfadd("dest", &format!("a{i}")).unwrap();
        }
        for i in 0..20 {
            c.pfadd("src", &format!("b{i}")).unwrap();
        }
        c.pfmerge("dest", &["src"]).unwrap();
        let count = c.pfcount("dest").unwrap();
        assert!(
            count >= 30 && count <= 50,
            "expected ~40, got {count}"
        );
    }

    #[test]
    fn hll_pfmerge_nonexistent_sources() {
        let c = ShardedCollections::new();
        c.pfadd("base", "x").unwrap();
        c.pfmerge("base", &["ghost1", "ghost2"]).unwrap();
        let count = c.pfcount("base").unwrap();
        assert!(count >= 1 && count <= 2);
    }

    #[test]
    fn hll_wrong_type() {
        let c = ShardedCollections::new();
        c.rpush("list", Value::Int32(1)).unwrap();
        let err = c.pfadd("list", "element").unwrap_err();
        assert_eq!(err.expected, "hyperloglog");
        assert_eq!(err.actual, "list");
    }

    // ------------------------------------------------------------------------
    // Cross-type and utility tests
    // ------------------------------------------------------------------------

    #[test]
    fn del_removes_any_type() {
        let c = ShardedCollections::new();
        c.rpush("list", Value::Int32(1)).unwrap();
        c.hset("hash", "f", Value::Int32(2)).unwrap();
        c.sadd("set", "m").unwrap();
        c.zadd("zset", "m", 1.0).unwrap();
        c.pfadd("hll", "e").unwrap();

        assert!(c.del("list"));
        assert!(c.del("hash"));
        assert!(c.del("set"));
        assert!(c.del("zset"));
        assert!(c.del("hll"));

        assert!(!c.exists("list"));
        assert!(!c.exists("hash"));
        assert!(!c.exists("set"));
        assert!(!c.exists("zset"));
        assert!(!c.exists("hll"));
    }

    #[test]
    fn del_nonexistent() {
        let c = ShardedCollections::new();
        assert!(!c.del("nothing"));
    }

    #[test]
    fn key_type_returns_correct_type() {
        let c = ShardedCollections::new();
        c.rpush("l", Value::Int32(1)).unwrap();
        c.hset("h", "f", Value::Int32(1)).unwrap();
        c.sadd("s", "m").unwrap();
        c.zadd("z", "m", 1.0).unwrap();
        c.pfadd("p", "e").unwrap();

        assert_eq!(c.key_type("l"), Some("list"));
        assert_eq!(c.key_type("h"), Some("hash"));
        assert_eq!(c.key_type("s"), Some("set"));
        assert_eq!(c.key_type("z"), Some("zset"));
        assert_eq!(c.key_type("p"), Some("hyperloglog"));
        assert_eq!(c.key_type("missing"), None);
    }

    #[test]
    fn exists_checks_any_type() {
        let c = ShardedCollections::new();
        assert!(!c.exists("k"));
        c.sadd("k", "v").unwrap();
        assert!(c.exists("k"));
    }

    #[test]
    fn wrong_type_error_display() {
        let err = WrongTypeError {
            expected: "list",
            actual: "set",
        };
        let msg = format!("{err}");
        assert!(msg.contains("WRONGTYPE"));
        assert!(msg.contains("list"));
        assert!(msg.contains("set"));
    }

    #[test]
    fn normalize_index_positive() {
        assert_eq!(normalize_index(0, 5), 0);
        assert_eq!(normalize_index(3, 5), 3);
        assert_eq!(normalize_index(10, 5), 10);
    }

    #[test]
    fn normalize_index_negative() {
        assert_eq!(normalize_index(-1, 5), 4);
        assert_eq!(normalize_index(-5, 5), 0);
        assert_eq!(normalize_index(-10, 5), 0);
    }

    #[test]
    fn shard_distribution() {
        let mut seen = std::collections::HashSet::new();
        for i in 0..200 {
            let idx = ShardedCollections::shard_index(&format!("key:{i}"));
            assert!(idx < NUM_SHARDS);
            seen.insert(idx);
        }
        assert!(
            seen.len() >= 30,
            "poor distribution: only {} shards used",
            seen.len()
        );
    }

    // ------------------------------------------------------------------------
    // Stress / concurrency tests
    // ------------------------------------------------------------------------

    #[test]
    fn concurrent_list_operations() {
        use std::sync::Arc;
        use std::thread;

        let c = Arc::new(ShardedCollections::new());
        let mut handles = vec![];

        for t in 0..8 {
            let c = Arc::clone(&c);
            handles.push(thread::spawn(move || {
                for i in 0..100 {
                    c.rpush("shared", Value::Int64((t * 100 + i) as i64)).unwrap();
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(c.llen("shared").unwrap(), 800);
    }

    #[test]
    fn concurrent_set_operations() {
        use std::sync::Arc;
        use std::thread;

        let c = Arc::new(ShardedCollections::new());
        let mut handles = vec![];

        for t in 0..4 {
            let c = Arc::clone(&c);
            handles.push(thread::spawn(move || {
                for i in 0..50 {
                    c.sadd("concurrent_set", &format!("t{t}_m{i}")).unwrap();
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(c.scard("concurrent_set").unwrap(), 200);
    }

    #[test]
    fn concurrent_hash_operations() {
        use std::sync::Arc;
        use std::thread;

        let c = Arc::new(ShardedCollections::new());
        let mut handles = vec![];

        for t in 0..4 {
            let c = Arc::clone(&c);
            handles.push(thread::spawn(move || {
                for i in 0..50 {
                    c.hset(
                        "concurrent_hash",
                        &format!("t{t}_f{i}"),
                        Value::Int32(i),
                    )
                    .unwrap();
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(c.hlen("concurrent_hash").unwrap(), 200);
    }
}
