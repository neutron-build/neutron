//! Write-ahead log for KV collection data structures.
//!
//! Provides crash-recovery by recording all collection mutations (Lists, Hashes,
//! Sets, Sorted Sets, HyperLogLog, Streams) to an append-only log file (`collections.wal`).
//! On restart the log is replayed to reconstruct in-memory state.
//!
//! ## Log entry binary format
//! ```text
//! Each entry: [op: u8] [key_len: u32 LE] [key: bytes] [data_len: u32 LE] [data: bytes]
//! ```
//!
//! A SNAPSHOT entry resets all collection state. After `checkpoint()` the file
//! is truncated to a single SNAPSHOT entry so the log stays small.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

use crate::kv::{HyperLogLog, SortedSet};
use crate::types::Value;

use super::collections::{GeoSet, KvCollection, ShardedCollections};
use super::streams::{Stream, StreamId};

// ─── Operation tags ──────────────────────────────────────────────────────────

const OP_LPUSH: u8 = 1;
const OP_RPUSH: u8 = 2;
const OP_LPOP: u8 = 3;
const OP_RPOP: u8 = 4;
const OP_HSET: u8 = 10;
const OP_HDEL: u8 = 11;
const OP_SADD: u8 = 20;
const OP_SREM: u8 = 21;
const OP_ZADD: u8 = 30;
const OP_ZREM: u8 = 31;
const OP_ZINCRBY: u8 = 32;
const OP_PFADD: u8 = 40;
const OP_PFMERGE: u8 = 41;
const OP_XADD: u8 = 45;
const OP_XDEL: u8 = 46;
const OP_DEL: u8 = 50;
const OP_SNAPSHOT: u8 = 60;

// ─── Value encoding (same scheme as kv_wal) ─────────────────────────────────

fn encode_value(val: &Value, buf: &mut Vec<u8>) {
    match val {
        Value::Null => buf.push(0),
        Value::Bool(b) => {
            buf.push(1);
            buf.push(*b as u8);
        }
        Value::Int32(n) => {
            buf.push(2);
            buf.extend_from_slice(&n.to_le_bytes());
        }
        Value::Int64(n) => {
            buf.push(3);
            buf.extend_from_slice(&n.to_le_bytes());
        }
        Value::Float64(f) => {
            buf.push(4);
            buf.extend_from_slice(&f.to_le_bytes());
        }
        Value::Text(s) => {
            buf.push(5);
            let b = s.as_bytes();
            buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
            buf.extend_from_slice(b);
        }
        other => {
            let s = format!("{other}");
            let b = s.as_bytes();
            buf.push(5);
            buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
            buf.extend_from_slice(b);
        }
    }
}

fn decode_value(data: &[u8], pos: &mut usize) -> Option<Value> {
    let tag = *data.get(*pos)?;
    *pos += 1;
    match tag {
        0 => Some(Value::Null),
        1 => {
            let b = *data.get(*pos)?;
            *pos += 1;
            Some(Value::Bool(b != 0))
        }
        2 => {
            let n = read_i32(data, pos)?;
            Some(Value::Int32(n))
        }
        3 => {
            let n = read_i64(data, pos)?;
            Some(Value::Int64(n))
        }
        4 => {
            let f = read_f64(data, pos)?;
            Some(Value::Float64(f))
        }
        5 => {
            let len = read_u32(data, pos)? as usize;
            if *pos + len > data.len() {
                return None;
            }
            let s = std::str::from_utf8(&data[*pos..*pos + len])
                .ok()?
                .to_string();
            *pos += len;
            Some(Value::Text(s))
        }
        _ => None,
    }
}

// ─── Primitive readers ──────────────────────────────────────────────────────

fn read_u32(data: &[u8], pos: &mut usize) -> Option<u32> {
    let b = data.get(*pos..*pos + 4)?;
    *pos += 4;
    Some(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
}

fn read_i32(data: &[u8], pos: &mut usize) -> Option<i32> {
    read_u32(data, pos).map(|u| u as i32)
}

fn read_u64(data: &[u8], pos: &mut usize) -> Option<u64> {
    let b = data.get(*pos..*pos + 8)?;
    *pos += 8;
    Some(u64::from_le_bytes([
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
    ]))
}

fn read_i64(data: &[u8], pos: &mut usize) -> Option<i64> {
    read_u64(data, pos).map(|v| v as i64)
}

fn read_f64(data: &[u8], pos: &mut usize) -> Option<f64> {
    read_u64(data, pos).map(f64::from_bits)
}

fn read_string(data: &[u8], pos: &mut usize) -> Option<String> {
    let len = read_u32(data, pos)? as usize;
    if *pos + len > data.len() {
        return None;
    }
    let s = std::str::from_utf8(&data[*pos..*pos + len])
        .ok()?
        .to_string();
    *pos += len;
    Some(s)
}

fn write_string(s: &str, buf: &mut Vec<u8>) {
    let b = s.as_bytes();
    buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
    buf.extend_from_slice(b);
}

// ─── Collection snapshot types (for serializing entire state) ───────────────

/// Collection type tags for snapshot serialization.
const COLL_LIST: u8 = 1;
const COLL_HASH: u8 = 2;
const COLL_SET: u8 = 3;
const COLL_ZSET: u8 = 4;
const COLL_HLL: u8 = 5;
const COLL_STREAM: u8 = 6;
const COLL_GEO: u8 = 7;

// ─── CollectionWal ──────────────────────────────────────────────────────────

/// Append-only WAL for KV collections.
pub struct CollectionWal {
    path: PathBuf,
    writer: Mutex<BufWriter<File>>,
    /// Group-commit fsync coordinator (durability of the un-checkpointed tail).
    syncer: crate::storage::wal_util::WalSync,
    /// Records appended but whose effect the caller has not applied yet.
    ///
    /// Every collection op logs BEFORE it takes its shard lock, so between the
    /// two the log holds a record the in-memory collections do not. A
    /// checkpoint that serialized memory in that window wrote a snapshot
    /// missing the record and then replaced the whole log with it — the write
    /// was acknowledged, gone from disk, and alive only in the memory of the
    /// process that had not applied it yet. `checkpoint` now waits for this to
    /// reach zero while holding the writer lock, which no new append can pass.
    in_flight: AtomicU64,
}

/// Outstanding-append guard: alive from the moment a record reaches the log
/// until the caller has applied its effect to the collections.
///
/// A checkpoint may not snapshot memory while any of these exist, because
/// memory is behind the log by exactly those records.
#[must_use = "hold this until the record's effect has been applied, or a \
              checkpoint can snapshot memory that is missing it"]
pub struct InFlight<'a> {
    wal: &'a CollectionWal,
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.wal.in_flight.fetch_sub(1, Ordering::AcqRel);
    }
}

impl CollectionWal {
    /// Open or create the collections WAL file in `dir`.
    ///
    /// Returns `(wal, recovered_collections)`. If no WAL file exists a fresh
    /// `ShardedCollections` is returned. A torn trailing record is still
    /// ignored — a crash can always half-write the last append, and dropping it
    /// is correct WAL behaviour.
    ///
    /// **Structural corruption is an error, not an empty result.** This used to
    /// return `Ok` no matter what replay found, so `Executor::open_durable` —
    /// which exists precisely to shout "this model is now VOLATILE" — could
    /// never fire for this store. Recovery could detect corruption and had no
    /// way to say so.
    pub fn open(dir: &Path) -> io::Result<(Self, ShardedCollections)> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("collections.wal");
        let collections = if path.exists() {
            let data = std::fs::read(&path)?;
            let outcome = replay(&data);
            if let Some(reason) = outcome.corruption {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("{}: {reason}", path.display()),
                ));
            }
            outcome.collections
        } else {
            ShardedCollections::new()
        };
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok((
            Self {
                path,
                writer: Mutex::new(BufWriter::new(file)),
                syncer: crate::storage::wal_util::WalSync::new(),
                in_flight: AtomicU64::new(0),
            },
            collections,
        ))
    }

    /// Mark an operation in flight before it is logged.
    ///
    /// The fourteen list/hash/set ops log BEFORE they apply, so `append`'s own
    /// guard covers their window. Stream ops cannot: `XADD` does not know the
    /// id it is logging until the entry is in the stream. They therefore take
    /// this guard first, apply, log, and drop it — so a checkpoint can no more
    /// run between their apply and their log than between the others' log and
    /// their apply. Without it, a checkpoint in that window snapshots the
    /// applied entry, truncates the log, and the append that follows lands in
    /// the fresh tail: replay then applies the same entry twice.
    pub fn begin_op(&self) -> InFlight<'_> {
        let _w = self.writer.lock();
        self.in_flight.fetch_add(1, Ordering::AcqRel);
        InFlight { wal: self }
    }

    /// Append a WAL entry: op(u8) + key_len(u32) + key + data_len(u32) + data.
    fn append(&self, op: u8, key: &str, data: &[u8]) -> io::Result<InFlight<'_>> {
        let mut buf = Vec::new();
        buf.push(op);
        write_string(key, &mut buf);
        buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
        buf.extend_from_slice(data);

        // The lock comes FIRST and the count is taken under it. Counting
        // before the lock deadlocks: a writer blocked on the lock would already
        // have incremented, and `checkpoint` holds that lock while waiting for
        // the count to reach zero. Under the lock, a writer that has not yet
        // entered cannot have incremented, so the count can always drain.
        let mut w = self.writer.lock();
        self.in_flight.fetch_add(1, Ordering::AcqRel);
        let write = w.write_all(&buf).and_then(|()| w.flush());
        if let Err(e) = write {
            self.in_flight.fetch_sub(1, Ordering::AcqRel);
            return Err(e);
        }
        self.syncer.on_append();
        Ok(InFlight { wal: self })
    }

    /// Flush + `fsync` the log, capturing (under the writer lock) the highest
    /// append LSN the fsync covers.
    fn sync_covering(&self) -> io::Result<u64> {
        let mut w = self.writer.lock();
        let covered = self.syncer.current();
        w.flush()?;
        w.get_ref().sync_all()?;
        Ok(covered)
    }

    /// Group-commit sync: durable coverage of every append made before this
    /// call; concurrent committers share fsyncs.
    pub fn group_sync(&self) -> io::Result<()> {
        self.syncer.group_sync(|| self.sync_covering())
    }

    /// Whether appends exist that no completed fsync covers yet.
    pub fn is_dirty(&self) -> bool {
        self.syncer.is_dirty()
    }

    // ── List ops ────────────────────────────────────────────────────────────

    pub fn log_lpush(&self, key: &str, value: &Value) -> io::Result<InFlight<'_>> {
        let mut data = Vec::new();
        encode_value(value, &mut data);
        self.append(OP_LPUSH, key, &data)
    }

    pub fn log_rpush(&self, key: &str, value: &Value) -> io::Result<InFlight<'_>> {
        let mut data = Vec::new();
        encode_value(value, &mut data);
        self.append(OP_RPUSH, key, &data)
    }

    pub fn log_lpop(&self, key: &str) -> io::Result<InFlight<'_>> {
        self.append(OP_LPOP, key, &[])
    }

    pub fn log_rpop(&self, key: &str) -> io::Result<InFlight<'_>> {
        self.append(OP_RPOP, key, &[])
    }

    // ── Hash ops ────────────────────────────────────────────────────────────

    pub fn log_hset(&self, key: &str, field: &str, value: &Value) -> io::Result<InFlight<'_>> {
        let mut data = Vec::new();
        write_string(field, &mut data);
        encode_value(value, &mut data);
        self.append(OP_HSET, key, &data)
    }

    pub fn log_hdel(&self, key: &str, field: &str) -> io::Result<InFlight<'_>> {
        let mut data = Vec::new();
        write_string(field, &mut data);
        self.append(OP_HDEL, key, &data)
    }

    // ── Set ops ─────────────────────────────────────────────────────────────

    pub fn log_sadd(&self, key: &str, member: &str) -> io::Result<InFlight<'_>> {
        let mut data = Vec::new();
        write_string(member, &mut data);
        self.append(OP_SADD, key, &data)
    }

    pub fn log_srem(&self, key: &str, member: &str) -> io::Result<InFlight<'_>> {
        let mut data = Vec::new();
        write_string(member, &mut data);
        self.append(OP_SREM, key, &data)
    }

    // ── Sorted Set ops ──────────────────────────────────────────────────────

    pub fn log_zadd(&self, key: &str, member: &str, score: f64) -> io::Result<InFlight<'_>> {
        let mut data = Vec::new();
        write_string(member, &mut data);
        data.extend_from_slice(&score.to_le_bytes());
        self.append(OP_ZADD, key, &data)
    }

    pub fn log_zrem(&self, key: &str, member: &str) -> io::Result<InFlight<'_>> {
        let mut data = Vec::new();
        write_string(member, &mut data);
        self.append(OP_ZREM, key, &data)
    }

    pub fn log_zincrby(&self, key: &str, member: &str, increment: f64) -> io::Result<InFlight<'_>> {
        let mut data = Vec::new();
        write_string(member, &mut data);
        data.extend_from_slice(&increment.to_le_bytes());
        self.append(OP_ZINCRBY, key, &data)
    }

    // ── HyperLogLog ops ─────────────────────────────────────────────────────

    pub fn log_pfadd(&self, key: &str, element: &str) -> io::Result<InFlight<'_>> {
        let mut data = Vec::new();
        write_string(element, &mut data);
        self.append(OP_PFADD, key, &data)
    }

    pub fn log_pfmerge(
        &self,
        dest_key: &str,
        source_registers: &[Vec<u8>],
    ) -> io::Result<InFlight<'_>> {
        let mut data = Vec::new();
        data.extend_from_slice(&(source_registers.len() as u32).to_le_bytes());
        for regs in source_registers {
            data.extend_from_slice(&(regs.len() as u32).to_le_bytes());
            data.extend_from_slice(regs);
        }
        self.append(OP_PFMERGE, dest_key, &data)
    }

    // ── Stream ops ──────────────────────────────────────────────────────────

    pub fn log_xadd(
        &self,
        key: &str,
        id: &StreamId,
        fields: &[(String, String)],
    ) -> io::Result<InFlight<'_>> {
        let mut data = Vec::new();
        data.extend_from_slice(&id.ms.to_le_bytes());
        data.extend_from_slice(&id.seq.to_le_bytes());
        data.extend_from_slice(&(fields.len() as u32).to_le_bytes());
        for (field, value) in fields {
            write_string(field, &mut data);
            write_string(value, &mut data);
        }
        self.append(OP_XADD, key, &data)
    }

    pub fn log_xdel(&self, key: &str, ids: &[StreamId]) -> io::Result<InFlight<'_>> {
        let mut data = Vec::new();
        data.extend_from_slice(&(ids.len() as u32).to_le_bytes());
        for id in ids {
            data.extend_from_slice(&id.ms.to_le_bytes());
            data.extend_from_slice(&id.seq.to_le_bytes());
        }
        self.append(OP_XDEL, key, &data)
    }

    // ── Housekeeping ────────────────────────────────────────────────────────

    pub fn log_del(&self, key: &str) -> io::Result<InFlight<'_>> {
        self.append(OP_DEL, key, &[])
    }

    /// Write a full snapshot of all collections and truncate the WAL.
    pub fn checkpoint(&self, collections: &ShardedCollections) -> io::Result<()> {
        // The writer lock comes FIRST, and the snapshot is taken under it.
        //
        // It used to be the other way round: `serialize_snapshot` read all 64
        // shards, and only then was the lock taken and the whole log replaced
        // by that snapshot. Anything appended in between was in neither — and
        // the `mark_synced` below then told the syncer every outstanding append
        // was durable, so nothing would ever rewrite it. An acknowledged write,
        // gone from disk, alive only in memory.
        let mut w = self.writer.lock();
        w.flush()?;

        // Then wait for the log to stop being ahead of memory. Every collection
        // op logs BEFORE it takes its shard lock, so a record can be on disk
        // with its effect not yet applied; snapshotting memory in that window
        // writes a snapshot that is missing it. No new append can start here —
        // `append` needs this same writer lock — so this drains rather than
        // chases.
        let mut spins = 0u32;
        while self.in_flight.load(Ordering::Acquire) > 0 {
            std::hint::spin_loop();
            spins += 1;
            // The apply that follows an append is a shard-lock acquisition and
            // a small mutation. If it has not happened after this many spins
            // the thread is descheduled, not deadlocked — yield rather than
            // burn the core.
            if spins.is_multiple_of(1024) {
                std::thread::yield_now();
            }
        }

        let snapshot_data = serialize_snapshot(collections);

        // Serialize the complete new log body (SNAPSHOT tag + empty key + snapshot).
        let mut contents: Vec<u8> = Vec::new();
        contents.push(OP_SNAPSHOT);
        write_string_to_writer("", &mut contents)?;
        contents.extend_from_slice(&(snapshot_data.len() as u32).to_le_bytes());
        contents.extend_from_slice(&snapshot_data);

        // Replace atomically — temp file + fsync + rename — so a crash
        // mid-checkpoint leaves the old log or the new snapshot, never an empty
        // file.
        crate::storage::wal_util::atomic_replace_wal(&self.path, &contents)?;
        let file = OpenOptions::new().append(true).open(&self.path)?;
        *w = BufWriter::new(file);
        // The snapshot was fsync'd by `atomic_replace_wal`; count it as covered.
        let mark = self.syncer.on_append();
        self.syncer.mark_synced(mark);
        Ok(())
    }
}

fn write_string_to_writer<W: Write>(s: &str, w: &mut W) -> io::Result<()> {
    let b = s.as_bytes();
    w.write_all(&(b.len() as u32).to_le_bytes())?;
    w.write_all(b)
}

// ─── Snapshot serialization ─────────────────────────────────────────────────
//
// Format: n_collections(u32) then per collection:
//   key_len(u32) + key + type_tag(u8) + type-specific data

fn serialize_snapshot(collections: &ShardedCollections) -> Vec<u8> {
    let all = collections.snapshot_all();
    let mut buf = Vec::new();
    buf.extend_from_slice(&(all.len() as u32).to_le_bytes());
    for (key, coll) in &all {
        write_string(key, &mut buf);
        match coll {
            KvCollection::List(list) => {
                buf.push(COLL_LIST);
                buf.extend_from_slice(&(list.len() as u32).to_le_bytes());
                for val in list {
                    encode_value(val, &mut buf);
                }
            }
            KvCollection::Hash(hash) => {
                buf.push(COLL_HASH);
                buf.extend_from_slice(&(hash.len() as u32).to_le_bytes());
                for (field, val) in hash {
                    write_string(field, &mut buf);
                    encode_value(val, &mut buf);
                }
            }
            KvCollection::Set(set) => {
                buf.push(COLL_SET);
                buf.extend_from_slice(&(set.len() as u32).to_le_bytes());
                for member in set {
                    write_string(member, &mut buf);
                }
            }
            KvCollection::SortedSet(zset) => {
                buf.push(COLL_ZSET);
                let card = zset.zcard();
                buf.extend_from_slice(&(card as u32).to_le_bytes());
                // Use zrange to get all entries in order
                let entries = zset.zrange(0, -1); // whole set
                for entry in &entries {
                    write_string(&entry.member, &mut buf);
                    buf.extend_from_slice(&entry.score.to_le_bytes());
                }
            }
            KvCollection::HyperLogLog(hll) => {
                buf.push(COLL_HLL);
                let regs = hll.registers();
                buf.extend_from_slice(&(regs.len() as u32).to_le_bytes());
                buf.extend_from_slice(regs);
            }
            KvCollection::Stream(stream) => {
                buf.push(COLL_STREAM);
                let entries = stream.xrange("-", "+", None);
                buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
                for entry in &entries {
                    // Write entry ID: ms(u64) + seq(u64)
                    buf.extend_from_slice(&entry.id.ms.to_le_bytes());
                    buf.extend_from_slice(&entry.id.seq.to_le_bytes());
                    // Write number of field-value pairs
                    buf.extend_from_slice(&(entry.fields.len() as u32).to_le_bytes());
                    for (field, value) in &entry.fields {
                        write_string(field, &mut buf);
                        write_string(value, &mut buf);
                    }
                }
            }
            KvCollection::Geo(geo) => {
                buf.push(COLL_GEO);
                let members = geo.members();
                buf.extend_from_slice(&(members.len() as u32).to_le_bytes());
                for (member, (lon, lat)) in members {
                    write_string(member, &mut buf);
                    buf.extend_from_slice(&lon.to_le_bytes());
                    buf.extend_from_slice(&lat.to_le_bytes());
                }
            }
        }
    }
    buf
}

use crate::storage::wal_util::MAX_PREALLOC;

fn deserialize_snapshot(data: &[u8], pos: &mut usize) -> Option<Vec<(String, KvCollection)>> {
    let n = read_u32(data, pos)? as usize;
    let mut result = Vec::with_capacity(n.min(MAX_PREALLOC));
    for _ in 0..n {
        let key = read_string(data, pos)?;
        let type_tag = *data.get(*pos)?;
        *pos += 1;
        let coll = match type_tag {
            COLL_LIST => {
                let len = read_u32(data, pos)? as usize;
                let mut list = VecDeque::with_capacity(len.min(MAX_PREALLOC));
                for _ in 0..len {
                    let val = decode_value(data, pos)?;
                    list.push_back(val);
                }
                KvCollection::List(list)
            }
            COLL_HASH => {
                let len = read_u32(data, pos)? as usize;
                let mut hash = HashMap::with_capacity(len.min(MAX_PREALLOC));
                for _ in 0..len {
                    let field = read_string(data, pos)?;
                    let val = decode_value(data, pos)?;
                    hash.insert(field, val);
                }
                KvCollection::Hash(hash)
            }
            COLL_SET => {
                let len = read_u32(data, pos)? as usize;
                let mut set = HashSet::with_capacity(len.min(MAX_PREALLOC));
                for _ in 0..len {
                    let member = read_string(data, pos)?;
                    set.insert(member);
                }
                KvCollection::Set(set)
            }
            COLL_ZSET => {
                let len = read_u32(data, pos)? as usize;
                let mut zset = SortedSet::new();
                for _ in 0..len {
                    let member = read_string(data, pos)?;
                    let score = read_f64(data, pos)?;
                    zset.zadd(&member, score);
                }
                KvCollection::SortedSet(zset)
            }
            COLL_HLL => {
                let len = read_u32(data, pos)? as usize;
                if *pos + len > data.len() {
                    return None;
                }
                let regs = &data[*pos..*pos + len];
                *pos += len;
                let mut hll = HyperLogLog::new();
                hll.set_registers(regs);
                KvCollection::HyperLogLog(hll)
            }
            COLL_STREAM => {
                let n_entries = read_u32(data, pos)? as usize;
                let mut stream = Stream::new();
                for _ in 0..n_entries {
                    let ms = read_u64(data, pos)?;
                    let seq = read_u64(data, pos)?;
                    let n_fields = read_u32(data, pos)? as usize;
                    let mut fields = Vec::with_capacity(n_fields.min(MAX_PREALLOC));
                    for _ in 0..n_fields {
                        let field = read_string(data, pos)?;
                        let value = read_string(data, pos)?;
                        fields.push((field, value));
                    }
                    let id_str = format!("{ms}-{seq}");
                    if let Err(e) = stream.xadd(&id_str, fields) {
                        tracing::warn!("WAL stream snapshot entry {id_str} failed: {e}");
                    }
                }
                KvCollection::Stream(stream)
            }
            COLL_GEO => {
                let len = read_u32(data, pos)? as usize;
                let mut geo = GeoSet::new();
                for _ in 0..len {
                    let member = read_string(data, pos)?;
                    let lon = read_f64(data, pos)?;
                    let lat = read_f64(data, pos)?;
                    geo.add(lon, lat, &member);
                }
                KvCollection::Geo(geo)
            }
            _ => return None,
        };
        result.push((key, coll));
    }
    Some(result)
}

// ─── Replay ─────────────────────────────────────────────────────────────────

/// Replay all entries in `data` to reconstruct collection state.
///
/// SNAPSHOT entries reset all state to their embedded snapshot. Only the last
/// SNAPSHOT and subsequent incremental entries matter in practice.
/// What replay produced, and whether it is trustworthy.
///
/// The signature used to be `fn replay(&[u8]) -> ShardedCollections` — no error
/// channel at all — so five separate corruption paths could only `break` and
/// return partial state indistinguishable from a clean end of log. That is the
/// structural root of this bug class: recovery could not report failure even
/// when it detected it.
struct ReplayOutcome {
    collections: ShardedCollections,
    /// `Some` when replay stopped on structural corruption rather than on a
    /// clean end of data. Recovery must not report success while this is set.
    ///
    /// A torn trailing record is NOT corruption: a WAL's last append can be
    /// half-written when the process dies, and discarding it is the documented,
    /// correct behaviour. What belongs here is a record whose framing is intact
    /// and whose contents are not — which no crash produces.
    corruption: Option<String>,
}

fn replay(data: &[u8]) -> ReplayOutcome {
    let collections = ShardedCollections::new();
    let mut corruption: Option<String> = None;
    let mut pos = 0usize;

    while pos < data.len() {
        // Where this record begins, so a corruption report can name the offset
        // rather than just the fact.
        let rec_start = pos;
        let Some(&op) = data.get(pos) else { break };
        pos += 1;

        // Read key (all entries have a key, even SNAPSHOT which uses empty key)
        let Some(key) = read_string(data, &mut pos) else {
            break;
        };
        // Read data payload
        let Some(data_len) = read_u32(data, &mut pos) else {
            break;
        };
        let data_len = data_len as usize;
        if pos + data_len > data.len() {
            break;
        }
        let payload = &data[pos..pos + data_len];
        pos += data_len;

        match op {
            OP_LPUSH => {
                let mut dpos = 0;
                if let Some(val) = decode_value(payload, &mut dpos)
                    && let Err(e) = collections.lpush(&key, val)
                {
                    tracing::warn!("KV WAL replay lpush({key}) failed: {e}");
                }
            }
            OP_RPUSH => {
                let mut dpos = 0;
                if let Some(val) = decode_value(payload, &mut dpos)
                    && let Err(e) = collections.rpush(&key, val)
                {
                    tracing::warn!("KV WAL replay rpush({key}) failed: {e}");
                }
            }
            OP_LPOP => {
                if let Err(e) = collections.lpop(&key) {
                    tracing::warn!("KV WAL replay lpop({key}) failed: {e}");
                }
            }
            OP_RPOP => {
                if let Err(e) = collections.rpop(&key) {
                    tracing::warn!("KV WAL replay rpop({key}) failed: {e}");
                }
            }
            OP_HSET => {
                let mut dpos = 0;
                if let (Some(field), Some(val)) = (
                    read_string(payload, &mut dpos),
                    decode_value(payload, &mut dpos),
                ) && let Err(e) = collections.hset(&key, &field, val)
                {
                    tracing::warn!("KV WAL replay hset({key}) failed: {e}");
                }
            }
            OP_HDEL => {
                let mut dpos = 0;
                if let Some(field) = read_string(payload, &mut dpos)
                    && let Err(e) = collections.hdel(&key, &field)
                {
                    tracing::warn!("KV WAL replay hdel({key}) failed: {e}");
                }
            }
            OP_SADD => {
                let mut dpos = 0;
                if let Some(member) = read_string(payload, &mut dpos)
                    && let Err(e) = collections.sadd(&key, &member)
                {
                    tracing::warn!("KV WAL replay sadd({key}) failed: {e}");
                }
            }
            OP_SREM => {
                let mut dpos = 0;
                if let Some(member) = read_string(payload, &mut dpos)
                    && let Err(e) = collections.srem(&key, &member)
                {
                    tracing::warn!("KV WAL replay srem({key}) failed: {e}");
                }
            }
            OP_ZADD => {
                let mut dpos = 0;
                if let Some(member) = read_string(payload, &mut dpos)
                    && let Some(score) = read_f64(payload, &mut dpos)
                {
                    let _ = collections.zadd(&key, &member, score);
                }
            }
            OP_ZREM => {
                let mut dpos = 0;
                if let Some(member) = read_string(payload, &mut dpos) {
                    let _ = collections.zrem(&key, &member);
                }
            }
            OP_ZINCRBY => {
                let mut dpos = 0;
                if let Some(member) = read_string(payload, &mut dpos)
                    && let Some(increment) = read_f64(payload, &mut dpos)
                {
                    let _ = collections.zincrby(&key, &member, increment);
                }
            }
            OP_PFADD => {
                let mut dpos = 0;
                if let Some(element) = read_string(payload, &mut dpos) {
                    let _ = collections.pfadd(&key, &element);
                }
            }
            OP_PFMERGE => {
                let mut dpos = 0;
                if let Some(n_sources) = read_u32(payload, &mut dpos) {
                    // Replay merge: create temporary HLLs from register snapshots
                    // and merge them into the destination.
                    for _ in 0..n_sources {
                        let Some(reg_len) = read_u32(payload, &mut dpos) else {
                            break;
                        };
                        let reg_len = reg_len as usize;
                        if dpos + reg_len > payload.len() {
                            break;
                        }
                        let regs = &payload[dpos..dpos + reg_len];
                        dpos += reg_len;
                        // Merge into destination using direct insert
                        collections.pfmerge_registers(&key, regs);
                    }
                }
            }
            OP_XADD => {
                let mut dpos = 0;
                if let (Some(ms), Some(seq)) =
                    (read_u64(payload, &mut dpos), read_u64(payload, &mut dpos))
                    && let Some(n_fields) = read_u32(payload, &mut dpos)
                {
                    let mut fields = Vec::with_capacity((n_fields as usize).min(MAX_PREALLOC));
                    let mut ok = true;
                    for _ in 0..n_fields {
                        match (
                            read_string(payload, &mut dpos),
                            read_string(payload, &mut dpos),
                        ) {
                            (Some(f), Some(v)) => fields.push((f, v)),
                            _ => {
                                ok = false;
                                break;
                            }
                        }
                    }
                    if ok {
                        let id_str = format!("{ms}-{seq}");
                        if let Err(e) = collections.xadd(&key, &id_str, fields) {
                            tracing::warn!("KV WAL replay xadd({key}) failed: {e}");
                        }
                    }
                }
            }
            OP_XDEL => {
                let mut dpos = 0;
                if let Some(n_ids) = read_u32(payload, &mut dpos) {
                    let mut ids = Vec::with_capacity((n_ids as usize).min(MAX_PREALLOC));
                    for _ in 0..n_ids {
                        if let (Some(ms), Some(seq)) =
                            (read_u64(payload, &mut dpos), read_u64(payload, &mut dpos))
                        {
                            ids.push(StreamId::new(ms, seq));
                        }
                    }
                    if let Err(e) = collections.xdel(&key, &ids) {
                        tracing::warn!("KV WAL replay xdel({key}) failed: {e}");
                    }
                }
            }
            OP_DEL => {
                collections.del(&key);
            }
            OP_SNAPSHOT => {
                // Deserialize BEFORE clearing. This used to clear first and
                // then try to parse, so a snapshot that failed to deserialize
                // left the collections empty AND — unlike the unknown-op arm
                // below — did not stop, so every later record was replayed onto
                // the emptied state. Measured: three keys checkpointed, one
                // appended after, snapshot payload corrupted, and recovery came
                // back holding only the fourth. A database that comes up
                // looking populated and is missing most of itself is worse than
                // one that comes up empty, because nothing about it looks wrong.
                let mut dpos = 0;
                match deserialize_snapshot(payload, &mut dpos) {
                    Some(items) => {
                        collections.clear_all();
                        for (k, coll) in items {
                            collections.insert_collection(&k, coll);
                        }
                    }
                    None => {
                        corruption = Some(format!(
                            "snapshot record at byte {rec_start} is framed correctly but did \
                             not deserialize ({} payload bytes); refusing to clear recovered \
                             state or replay past it",
                            payload.len()
                        ));
                        break;
                    }
                }
            }
            other => {
                // Unknown op — we cannot know how much to skip, so replay stops
                // here and everything after it is discarded. That is data loss,
                // and it used to be silent.
                corruption = Some(format!(
                    "unknown opcode {other} at byte {rec_start}; replay stopped and the \
                     remaining {} byte(s) of log were discarded",
                    data.len().saturating_sub(rec_start)
                ));
                break;
            }
        }
    }

    ReplayOutcome {
        collections,
        corruption,
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    // 3.14/3.14159 here are arbitrary test fixtures, not PI approximations.
    #![allow(clippy::approx_constant)]
    use super::*;
    use crate::types::Value;

    #[test]
    fn group_sync_marks_clean() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _colls) = CollectionWal::open(dir.path()).unwrap();
        assert!(!wal.is_dirty(), "a fresh WAL has no un-fsynced appends");
        drop(wal.log_lpush("l", &Value::Text("a".into())).unwrap());
        assert!(wal.is_dirty(), "an append is uncovered until fsync");
        wal.group_sync().unwrap();
        assert!(!wal.is_dirty(), "group_sync fsyncs the tail");
    }

    /// NU-384: a malformed snapshot used to clear the collections BEFORE it was
    /// validated and then keep replaying, so recovery came back holding a
    /// mosaic — some keys, silently missing the rest, reporting success.
    ///
    /// Measured on the old code: three keys checkpointed, a fourth appended
    /// after the snapshot, then the snapshot payload corrupted. Recovery
    /// returned `Ok` holding exactly `["delta"]` — the post-snapshot append,
    /// replayed onto state the failed snapshot had already emptied.
    #[test]
    fn a_malformed_snapshot_is_reported_and_never_clears_recovered_state() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, colls) = CollectionWal::open(dir.path()).unwrap();

        for k in ["alpha", "beta", "gamma"] {
            colls.lpush(k, Value::Text("v".into())).unwrap();
            drop(wal.log_lpush(k, &Value::Text("v".into())).unwrap());
        }
        wal.checkpoint(&colls).unwrap();

        colls.lpush("delta", Value::Text("v".into())).unwrap();
        drop(wal.log_lpush("delta", &Value::Text("v".into())).unwrap());
        wal.group_sync().unwrap();
        drop(wal);

        // A clean reopen sees all four, so the fixture is real.
        {
            let (_w, c) = CollectionWal::open(dir.path()).unwrap();
            for k in ["alpha", "beta", "gamma", "delta"] {
                assert_eq!(c.llen(k).unwrap(), 1, "clean reopen should hold {k}");
            }
        }

        // Corrupt ONLY the snapshot payload; the record framing stays valid, so
        // this is not a torn tail. Payload begins after op(1) + key_len(4) +
        // empty key(0) + payload_len(4).
        let path = dir.path().join("collections.wal");
        let mut bytes = std::fs::read(&path).unwrap();
        let payload_start = 1 + 4 + 4;
        bytes[payload_start..payload_start + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        std::fs::write(&path, &bytes).unwrap();

        let err = match CollectionWal::open(dir.path()) {
            Ok(_) => panic!("a snapshot that does not deserialize must not open successfully"),
            Err(e) => e,
        };
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        let msg = err.to_string();
        assert!(
            msg.contains("snapshot record"),
            "the error should name what was wrong, got: {msg}"
        );
    }

    /// The torn tail stays non-fatal. A crash can always half-write the last
    /// append, and turning that into a refusal to open would make ordinary
    /// crash recovery fail — a fix that did so would still pass the test above.
    #[test]
    fn a_torn_trailing_record_is_not_treated_as_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, colls) = CollectionWal::open(dir.path()).unwrap();
        for k in ["alpha", "beta"] {
            colls.lpush(k, Value::Text("v".into())).unwrap();
            drop(wal.log_lpush(k, &Value::Text("v".into())).unwrap());
        }
        wal.group_sync().unwrap();
        drop(wal);

        // Chop the last few bytes: the final record is now incomplete.
        let path = dir.path().join("collections.wal");
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.truncate(bytes.len() - 3);
        std::fs::write(&path, &bytes).unwrap();

        let (_w, c) = CollectionWal::open(dir.path())
            .expect("a torn trailing record is normal crash recovery, not corruption");
        assert_eq!(c.llen("alpha").unwrap(), 1, "the intact record survives");
    }

    /// An unknown opcode discards everything after it. That is data loss, and
    /// it used to happen silently.
    #[test]
    fn an_unknown_opcode_is_reported_rather_than_silently_truncating() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, colls) = CollectionWal::open(dir.path()).unwrap();
        colls.lpush("alpha", Value::Text("v".into())).unwrap();
        drop(wal.log_lpush("alpha", &Value::Text("v".into())).unwrap());
        wal.group_sync().unwrap();
        drop(wal);

        let path = dir.path().join("collections.wal");
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[0] = 250; // not a defined opcode
        std::fs::write(&path, &bytes).unwrap();

        let err = match CollectionWal::open(dir.path()) {
            Ok(_) => panic!("an unknown opcode discards the rest of the log and must say so"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("unknown opcode"), "got: {err}");
    }

    /// NU-385: counts come out of the file, and a Rust allocation failure
    /// aborts rather than erroring — so an unbounded `with_capacity` driven by
    /// a corrupt `u32` is a boot crash-loop with no diagnostic. The snapshot
    /// above already claims `u32::MAX` items; this asserts the parse declines
    /// instead of trying to reserve ~4.3 billion elements.
    #[test]
    fn a_huge_declared_count_does_not_preallocate() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&u32::MAX.to_le_bytes()); // item count
        // ...and nothing else. Honest data would follow.
        let mut pos = 0;
        assert!(
            deserialize_snapshot(&payload, &mut pos).is_none(),
            "a count with no data behind it must fail the parse"
        );
    }

    #[test]
    fn test_list_wal_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, colls) = CollectionWal::open(dir.path()).unwrap();
        assert_eq!(colls.llen("mylist").unwrap(), 0);

        colls.lpush("mylist", Value::Text("a".into())).unwrap();
        drop(wal.log_lpush("mylist", &Value::Text("a".into())).unwrap());

        colls.rpush("mylist", Value::Text("b".into())).unwrap();
        drop(wal.log_rpush("mylist", &Value::Text("b".into())).unwrap());

        colls.lpush("mylist", Value::Text("c".into())).unwrap();
        drop(wal.log_lpush("mylist", &Value::Text("c".into())).unwrap());

        drop(wal);

        let (_wal2, colls2) = CollectionWal::open(dir.path()).unwrap();
        assert_eq!(colls2.llen("mylist").unwrap(), 3);
        let items = colls2.lrange("mylist", 0, -1).unwrap();
        assert_eq!(
            items,
            vec![
                Value::Text("c".into()),
                Value::Text("a".into()),
                Value::Text("b".into()),
            ]
        );
    }

    #[test]
    fn test_list_pop_wal_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, colls) = CollectionWal::open(dir.path()).unwrap();

        for v in ["a", "b", "c", "d"] {
            colls.rpush("q", Value::Text(v.into())).unwrap();
            drop(wal.log_rpush("q", &Value::Text(v.into())).unwrap());
        }

        colls.lpop("q").unwrap();
        drop(wal.log_lpop("q").unwrap());
        colls.rpop("q").unwrap();
        drop(wal.log_rpop("q").unwrap());

        drop(wal);

        let (_wal2, colls2) = CollectionWal::open(dir.path()).unwrap();
        let items = colls2.lrange("q", 0, -1).unwrap();
        assert_eq!(
            items,
            vec![Value::Text("b".into()), Value::Text("c".into())]
        );
    }

    #[test]
    fn test_hash_wal_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, colls) = CollectionWal::open(dir.path()).unwrap();

        colls
            .hset("user", "name", Value::Text("Alice".into()))
            .unwrap();
        drop(
            wal.log_hset("user", "name", &Value::Text("Alice".into()))
                .unwrap(),
        );

        colls.hset("user", "age", Value::Int32(30)).unwrap();
        drop(wal.log_hset("user", "age", &Value::Int32(30)).unwrap());

        colls.hdel("user", "age").unwrap();
        drop(wal.log_hdel("user", "age").unwrap());

        drop(wal);

        let (_wal2, colls2) = CollectionWal::open(dir.path()).unwrap();
        assert_eq!(
            colls2.hget("user", "name").unwrap(),
            Some(Value::Text("Alice".into()))
        );
        assert_eq!(colls2.hget("user", "age").unwrap(), None);
        assert_eq!(colls2.hlen("user").unwrap(), 1);
    }

    #[test]
    fn test_set_wal_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, colls) = CollectionWal::open(dir.path()).unwrap();

        colls.sadd("tags", "rust").unwrap();
        drop(wal.log_sadd("tags", "rust").unwrap());
        colls.sadd("tags", "mojo").unwrap();
        drop(wal.log_sadd("tags", "mojo").unwrap());
        colls.sadd("tags", "python").unwrap();
        drop(wal.log_sadd("tags", "python").unwrap());

        colls.srem("tags", "python").unwrap();
        drop(wal.log_srem("tags", "python").unwrap());

        drop(wal);

        let (_wal2, colls2) = CollectionWal::open(dir.path()).unwrap();
        let members = colls2.smembers("tags").unwrap();
        assert_eq!(members, vec!["mojo", "rust"]);
    }

    #[test]
    fn test_sortedset_wal_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, colls) = CollectionWal::open(dir.path()).unwrap();

        colls.zadd("lb", "alice", 100.0).unwrap();
        drop(wal.log_zadd("lb", "alice", 100.0).unwrap());
        colls.zadd("lb", "bob", 200.0).unwrap();
        drop(wal.log_zadd("lb", "bob", 200.0).unwrap());
        colls.zadd("lb", "charlie", 150.0).unwrap();
        drop(wal.log_zadd("lb", "charlie", 150.0).unwrap());

        colls.zrem("lb", "bob").unwrap();
        drop(wal.log_zrem("lb", "bob").unwrap());

        drop(wal);

        let (_wal2, colls2) = CollectionWal::open(dir.path()).unwrap();
        assert_eq!(colls2.zcard("lb").unwrap(), 2);
        let entries = colls2.zrange("lb", 0, 1).unwrap();
        assert_eq!(entries[0].member, "alice");
        assert_eq!(entries[1].member, "charlie");
    }

    #[test]
    fn test_sortedset_zincrby_wal_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, colls) = CollectionWal::open(dir.path()).unwrap();

        colls.zadd("z", "m", 10.0).unwrap();
        drop(wal.log_zadd("z", "m", 10.0).unwrap());
        colls.zincrby("z", "m", 5.0).unwrap();
        drop(wal.log_zincrby("z", "m", 5.0).unwrap());

        drop(wal);

        let (_wal2, colls2) = CollectionWal::open(dir.path()).unwrap();
        let entries = colls2.zrange("z", 0, 0).unwrap();
        assert_eq!(entries[0].score, 15.0);
    }

    #[test]
    fn test_hll_wal_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, colls) = CollectionWal::open(dir.path()).unwrap();

        for i in 0..50 {
            let elem = format!("user{i}");
            colls.pfadd("visitors", &elem).unwrap();
            drop(wal.log_pfadd("visitors", &elem).unwrap());
        }

        drop(wal);

        let (_wal2, colls2) = CollectionWal::open(dir.path()).unwrap();
        let count = colls2.pfcount("visitors").unwrap();
        assert!((40..=60).contains(&count), "expected ~50, got {count}");
    }

    #[test]
    fn test_hll_pfmerge_wal_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, colls) = CollectionWal::open(dir.path()).unwrap();

        for i in 0..30 {
            colls.pfadd("hll1", &format!("a{i}")).unwrap();
            drop(wal.log_pfadd("hll1", &format!("a{i}")).unwrap());
        }
        for i in 0..30 {
            colls.pfadd("hll2", &format!("b{i}")).unwrap();
            drop(wal.log_pfadd("hll2", &format!("b{i}")).unwrap());
        }

        // Capture source registers for WAL logging
        let src_regs = colls.get_hll_registers(&["hll1", "hll2"]);
        colls.pfmerge("merged", &["hll1", "hll2"]).unwrap();
        drop(wal.log_pfmerge("merged", &src_regs).unwrap());

        drop(wal);

        let (_wal2, colls2) = CollectionWal::open(dir.path()).unwrap();
        let count = colls2.pfcount("merged").unwrap();
        assert!((50..=70).contains(&count), "expected ~60, got {count}");
    }

    #[test]
    fn test_del_wal_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, colls) = CollectionWal::open(dir.path()).unwrap();

        colls.rpush("mylist", Value::Int32(1)).unwrap();
        drop(wal.log_rpush("mylist", &Value::Int32(1)).unwrap());
        colls.sadd("myset", "a").unwrap();
        drop(wal.log_sadd("myset", "a").unwrap());

        colls.del("mylist");
        drop(wal.log_del("mylist").unwrap());

        drop(wal);

        let (_wal2, colls2) = CollectionWal::open(dir.path()).unwrap();
        assert!(!colls2.exists("mylist"));
        assert!(colls2.exists("myset"));
    }

    #[test]
    fn test_checkpoint_and_continue() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, colls) = CollectionWal::open(dir.path()).unwrap();

        // Initial data
        colls.rpush("list", Value::Int32(1)).unwrap();
        drop(wal.log_rpush("list", &Value::Int32(1)).unwrap());
        colls.rpush("list", Value::Int32(2)).unwrap();
        drop(wal.log_rpush("list", &Value::Int32(2)).unwrap());
        colls.sadd("set", "x").unwrap();
        drop(wal.log_sadd("set", "x").unwrap());

        // Checkpoint
        wal.checkpoint(&colls).unwrap();

        // More mutations after checkpoint
        colls.rpush("list", Value::Int32(3)).unwrap();
        drop(wal.log_rpush("list", &Value::Int32(3)).unwrap());
        colls.sadd("set", "y").unwrap();
        drop(wal.log_sadd("set", "y").unwrap());

        drop(wal);

        let (_wal2, colls2) = CollectionWal::open(dir.path()).unwrap();
        let items = colls2.lrange("list", 0, -1).unwrap();
        assert_eq!(
            items,
            vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)]
        );
        let members = colls2.smembers("set").unwrap();
        assert_eq!(members, vec!["x", "y"]);
    }

    #[test]
    fn test_checkpoint_truncates_wal() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, colls) = CollectionWal::open(dir.path()).unwrap();

        // Write many entries
        for i in 0..100 {
            colls.rpush("big", Value::Int64(i)).unwrap();
            drop(wal.log_rpush("big", &Value::Int64(i)).unwrap());
        }
        let size_before = std::fs::metadata(dir.path().join("collections.wal"))
            .unwrap()
            .len();

        // Delete most, then checkpoint
        for _ in 0..95 {
            colls.lpop("big").unwrap();
        }
        wal.checkpoint(&colls).unwrap();
        let size_after = std::fs::metadata(dir.path().join("collections.wal"))
            .unwrap()
            .len();
        assert!(size_after < size_before, "checkpoint should shrink WAL");
    }

    #[test]
    fn test_corrupt_trailing_bytes_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, colls) = CollectionWal::open(dir.path()).unwrap();

        colls.sadd("good", "value").unwrap();
        drop(wal.log_sadd("good", "value").unwrap());
        drop(wal);

        // Append garbage bytes
        let wal_path = dir.path().join("collections.wal");
        let mut file = OpenOptions::new().append(true).open(&wal_path).unwrap();
        file.write_all(&[0xFF, 0xFE, 0xFD, 0x00, 0x01]).unwrap();
        file.flush().unwrap();
        drop(file);

        let (_wal2, colls2) = CollectionWal::open(dir.path()).unwrap();
        assert!(colls2.sismember("good", "value").unwrap());
    }

    #[test]
    fn test_empty_wal_fresh_start() {
        let dir = tempfile::tempdir().unwrap();
        let (_wal, colls) = CollectionWal::open(dir.path()).unwrap();
        assert_eq!(colls.llen("anything").unwrap(), 0);
        assert_eq!(colls.hlen("anything").unwrap(), 0);
        assert_eq!(colls.scard("anything").unwrap(), 0);
        assert_eq!(colls.zcard("anything").unwrap(), 0);
        assert_eq!(colls.pfcount("anything").unwrap(), 0);
    }

    #[test]
    fn test_close_reopen_preserves_all_types() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, colls) = CollectionWal::open(dir.path()).unwrap();

        // List
        colls.rpush("mylist", Value::Text("hello".into())).unwrap();
        drop(
            wal.log_rpush("mylist", &Value::Text("hello".into()))
                .unwrap(),
        );

        // Hash
        colls.hset("myhash", "field1", Value::Int64(42)).unwrap();
        drop(wal.log_hset("myhash", "field1", &Value::Int64(42)).unwrap());

        // Set
        colls.sadd("myset", "member1").unwrap();
        drop(wal.log_sadd("myset", "member1").unwrap());

        // Sorted Set
        colls.zadd("myzset", "player1", 99.5).unwrap();
        drop(wal.log_zadd("myzset", "player1", 99.5).unwrap());

        // HyperLogLog
        colls.pfadd("myhll", "item1").unwrap();
        drop(wal.log_pfadd("myhll", "item1").unwrap());

        drop(wal);

        // Reopen and verify all types survived
        let (_wal2, colls2) = CollectionWal::open(dir.path()).unwrap();

        let items = colls2.lrange("mylist", 0, -1).unwrap();
        assert_eq!(items, vec![Value::Text("hello".into())]);

        assert_eq!(
            colls2.hget("myhash", "field1").unwrap(),
            Some(Value::Int64(42))
        );

        assert!(colls2.sismember("myset", "member1").unwrap());

        let entries = colls2.zrange("myzset", 0, 0).unwrap();
        assert_eq!(entries[0].member, "player1");
        assert_eq!(entries[0].score, 99.5);

        let count = colls2.pfcount("myhll").unwrap();
        assert!(count >= 1);
    }

    #[test]
    fn test_mixed_operations_interleaved() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, colls) = CollectionWal::open(dir.path()).unwrap();

        // Interleave different collection types
        colls.rpush("list", Value::Int32(1)).unwrap();
        drop(wal.log_rpush("list", &Value::Int32(1)).unwrap());

        colls.sadd("set", "a").unwrap();
        drop(wal.log_sadd("set", "a").unwrap());

        colls.rpush("list", Value::Int32(2)).unwrap();
        drop(wal.log_rpush("list", &Value::Int32(2)).unwrap());

        colls.hset("hash", "k", Value::Text("v".into())).unwrap();
        drop(wal.log_hset("hash", "k", &Value::Text("v".into())).unwrap());

        colls.zadd("zset", "m1", 1.0).unwrap();
        drop(wal.log_zadd("zset", "m1", 1.0).unwrap());

        colls.sadd("set", "b").unwrap();
        drop(wal.log_sadd("set", "b").unwrap());

        colls.pfadd("hll", "x").unwrap();
        drop(wal.log_pfadd("hll", "x").unwrap());

        colls.rpush("list", Value::Int32(3)).unwrap();
        drop(wal.log_rpush("list", &Value::Int32(3)).unwrap());

        colls.zadd("zset", "m2", 2.0).unwrap();
        drop(wal.log_zadd("zset", "m2", 2.0).unwrap());

        drop(wal);

        let (_wal2, colls2) = CollectionWal::open(dir.path()).unwrap();

        // Verify all
        let items = colls2.lrange("list", 0, -1).unwrap();
        assert_eq!(
            items,
            vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)]
        );

        let members = colls2.smembers("set").unwrap();
        assert_eq!(members, vec!["a", "b"]);

        assert_eq!(
            colls2.hget("hash", "k").unwrap(),
            Some(Value::Text("v".into()))
        );

        let entries = colls2.zrange("zset", 0, 1).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].member, "m1");
        assert_eq!(entries[1].member, "m2");

        assert!(colls2.pfcount("hll").unwrap() >= 1);
    }

    #[test]
    fn test_snapshot_then_incremental() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, colls) = CollectionWal::open(dir.path()).unwrap();

        // Build initial state and checkpoint
        colls.rpush("list", Value::Int32(10)).unwrap();
        colls.sadd("set", "x").unwrap();
        colls.zadd("zset", "alice", 50.0).unwrap();
        wal.checkpoint(&colls).unwrap();

        // Incremental ops after checkpoint
        colls.rpush("list", Value::Int32(20)).unwrap();
        drop(wal.log_rpush("list", &Value::Int32(20)).unwrap());
        colls.del("set");
        drop(wal.log_del("set").unwrap());
        colls.zadd("zset", "bob", 60.0).unwrap();
        drop(wal.log_zadd("zset", "bob", 60.0).unwrap());

        drop(wal);

        let (_wal2, colls2) = CollectionWal::open(dir.path()).unwrap();
        let items = colls2.lrange("list", 0, -1).unwrap();
        assert_eq!(items, vec![Value::Int32(10), Value::Int32(20)]);
        assert!(!colls2.exists("set"));
        assert_eq!(colls2.zcard("zset").unwrap(), 2);
    }

    #[test]
    fn test_value_types_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, colls) = CollectionWal::open(dir.path()).unwrap();

        let values = [
            Value::Null,
            Value::Bool(true),
            Value::Bool(false),
            Value::Int32(-42),
            Value::Int64(i64::MAX),
            Value::Float64(3.14159),
            Value::Text("hello world".into()),
        ];

        for (i, val) in values.iter().enumerate() {
            colls.rpush("vals", val.clone()).unwrap();
            drop(wal.log_rpush("vals", val).unwrap());
            colls.hset("hvals", &format!("f{i}"), val.clone()).unwrap();
            drop(wal.log_hset("hvals", &format!("f{i}"), val).unwrap());
        }

        drop(wal);

        let (_wal2, colls2) = CollectionWal::open(dir.path()).unwrap();
        let items = colls2.lrange("vals", 0, -1).unwrap();
        assert_eq!(items.len(), values.len());
        for (got, expected) in items.iter().zip(values.iter()) {
            assert_eq!(got, expected);
        }
    }

    #[test]
    fn test_checkpoint_with_all_types() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, colls) = CollectionWal::open(dir.path()).unwrap();

        // Create all types
        colls.rpush("l", Value::Text("a".into())).unwrap();
        colls.lpush("l", Value::Text("b".into())).unwrap();
        colls.hset("h", "f1", Value::Int32(1)).unwrap();
        colls.hset("h", "f2", Value::Int32(2)).unwrap();
        colls.sadd("s", "x").unwrap();
        colls.sadd("s", "y").unwrap();
        colls.zadd("z", "m1", 10.0).unwrap();
        colls.zadd("z", "m2", 20.0).unwrap();
        for i in 0..10 {
            colls.pfadd("p", &format!("elem{i}")).unwrap();
        }

        // Checkpoint
        wal.checkpoint(&colls).unwrap();
        drop(wal);

        // Reopen from checkpoint
        let (_wal2, colls2) = CollectionWal::open(dir.path()).unwrap();

        // Verify all types
        let list = colls2.lrange("l", 0, -1).unwrap();
        assert_eq!(list, vec![Value::Text("b".into()), Value::Text("a".into())]);

        assert_eq!(colls2.hget("h", "f1").unwrap(), Some(Value::Int32(1)));
        assert_eq!(colls2.hget("h", "f2").unwrap(), Some(Value::Int32(2)));

        let members = colls2.smembers("s").unwrap();
        assert_eq!(members, vec!["x", "y"]);

        let entries = colls2.zrange("z", 0, 1).unwrap();
        assert_eq!(entries[0].member, "m1");
        assert_eq!(entries[0].score, 10.0);
        assert_eq!(entries[1].member, "m2");

        let count = colls2.pfcount("p").unwrap();
        assert!((8..=12).contains(&count), "expected ~10, got {count}");
    }

    #[test]
    fn test_multiple_checkpoints() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, colls) = CollectionWal::open(dir.path()).unwrap();

        colls.sadd("s", "a").unwrap();
        wal.checkpoint(&colls).unwrap();

        colls.sadd("s", "b").unwrap();
        drop(wal.log_sadd("s", "b").unwrap());
        wal.checkpoint(&colls).unwrap();

        colls.sadd("s", "c").unwrap();
        drop(wal.log_sadd("s", "c").unwrap());

        drop(wal);

        let (_wal2, colls2) = CollectionWal::open(dir.path()).unwrap();
        let members = colls2.smembers("s").unwrap();
        assert_eq!(members, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_stream_wal_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, colls) = CollectionWal::open(dir.path()).unwrap();

        let id1 = colls
            .xadd(
                "mystream",
                "1-0",
                vec![
                    ("name".into(), "Alice".into()),
                    ("action".into(), "login".into()),
                ],
            )
            .unwrap();
        drop(
            wal.log_xadd(
                "mystream",
                &id1,
                &[
                    ("name".into(), "Alice".into()),
                    ("action".into(), "login".into()),
                ],
            )
            .unwrap(),
        );

        let id2 = colls
            .xadd(
                "mystream",
                "2-0",
                vec![
                    ("name".into(), "Bob".into()),
                    ("action".into(), "purchase".into()),
                ],
            )
            .unwrap();
        drop(
            wal.log_xadd(
                "mystream",
                &id2,
                &[
                    ("name".into(), "Bob".into()),
                    ("action".into(), "purchase".into()),
                ],
            )
            .unwrap(),
        );

        let id3 = colls
            .xadd("mystream", "3-0", vec![("name".into(), "Charlie".into())])
            .unwrap();
        drop(
            wal.log_xadd("mystream", &id3, &[("name".into(), "Charlie".into())])
                .unwrap(),
        );

        drop(wal);

        let (_wal2, colls2) = CollectionWal::open(dir.path()).unwrap();
        assert_eq!(colls2.xlen("mystream").unwrap(), 3);
        let entries = colls2.xrange("mystream", "-", "+", None).unwrap();
        assert_eq!(entries[0].id, StreamId::new(1, 0));
        assert_eq!(
            entries[0].fields,
            vec![
                ("name".into(), "Alice".into()),
                ("action".into(), "login".into()),
            ]
        );
        assert_eq!(entries[1].id, StreamId::new(2, 0));
        assert_eq!(entries[2].id, StreamId::new(3, 0));
        assert_eq!(entries[2].fields, vec![("name".into(), "Charlie".into())]);
    }

    #[test]
    fn test_stream_xdel_wal_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, colls) = CollectionWal::open(dir.path()).unwrap();

        let id1 = colls
            .xadd("s", "1-0", vec![("a".into(), "1".into())])
            .unwrap();
        drop(
            wal.log_xadd("s", &id1, &[("a".into(), "1".into())])
                .unwrap(),
        );
        let id2 = colls
            .xadd("s", "2-0", vec![("a".into(), "2".into())])
            .unwrap();
        drop(
            wal.log_xadd("s", &id2, &[("a".into(), "2".into())])
                .unwrap(),
        );
        let id3 = colls
            .xadd("s", "3-0", vec![("a".into(), "3".into())])
            .unwrap();
        drop(
            wal.log_xadd("s", &id3, &[("a".into(), "3".into())])
                .unwrap(),
        );

        colls.xdel("s", &[StreamId::new(2, 0)]).unwrap();
        drop(wal.log_xdel("s", &[StreamId::new(2, 0)]).unwrap());

        drop(wal);

        let (_wal2, colls2) = CollectionWal::open(dir.path()).unwrap();
        assert_eq!(colls2.xlen("s").unwrap(), 2);
        let entries = colls2.xrange("s", "-", "+", None).unwrap();
        assert_eq!(entries[0].id, StreamId::new(1, 0));
        assert_eq!(entries[1].id, StreamId::new(3, 0));
    }

    #[test]
    fn test_stream_checkpoint_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, colls) = CollectionWal::open(dir.path()).unwrap();

        colls
            .xadd(
                "events",
                "10-0",
                vec![
                    ("type".into(), "click".into()),
                    ("x".into(), "100".into()),
                    ("y".into(), "200".into()),
                ],
            )
            .unwrap();
        colls
            .xadd(
                "events",
                "20-0",
                vec![
                    ("type".into(), "scroll".into()),
                    ("delta".into(), "50".into()),
                ],
            )
            .unwrap();
        colls
            .xadd(
                "events",
                "30-5",
                vec![
                    ("type".into(), "keypress".into()),
                    ("key".into(), "Enter".into()),
                ],
            )
            .unwrap();

        // Checkpoint and reopen
        wal.checkpoint(&colls).unwrap();
        drop(wal);

        let (_wal2, colls2) = CollectionWal::open(dir.path()).unwrap();
        assert_eq!(colls2.xlen("events").unwrap(), 3);

        let entries = colls2.xrange("events", "-", "+", None).unwrap();
        assert_eq!(entries[0].id, StreamId::new(10, 0));
        assert_eq!(
            entries[0].fields,
            vec![
                ("type".into(), "click".into()),
                ("x".into(), "100".into()),
                ("y".into(), "200".into()),
            ]
        );
        assert_eq!(entries[1].id, StreamId::new(20, 0));
        assert_eq!(
            entries[1].fields,
            vec![
                ("type".into(), "scroll".into()),
                ("delta".into(), "50".into()),
            ]
        );
        assert_eq!(entries[2].id, StreamId::new(30, 5));
        assert_eq!(
            entries[2].fields,
            vec![
                ("type".into(), "keypress".into()),
                ("key".into(), "Enter".into()),
            ]
        );
    }

    #[test]
    fn test_stream_checkpoint_then_incremental() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, colls) = CollectionWal::open(dir.path()).unwrap();

        // Initial stream data
        colls
            .xadd("log", "1-0", vec![("msg".into(), "first".into())])
            .unwrap();
        colls
            .xadd("log", "2-0", vec![("msg".into(), "second".into())])
            .unwrap();

        // Checkpoint
        wal.checkpoint(&colls).unwrap();

        // More entries after checkpoint
        let id3 = colls
            .xadd("log", "3-0", vec![("msg".into(), "third".into())])
            .unwrap();
        drop(
            wal.log_xadd("log", &id3, &[("msg".into(), "third".into())])
                .unwrap(),
        );

        drop(wal);

        let (_wal2, colls2) = CollectionWal::open(dir.path()).unwrap();
        assert_eq!(colls2.xlen("log").unwrap(), 3);
        let entries = colls2.xrange("log", "-", "+", None).unwrap();
        assert_eq!(entries[0].fields[0].1, "first");
        assert_eq!(entries[1].fields[0].1, "second");
        assert_eq!(entries[2].fields[0].1, "third");
    }

    #[test]
    fn test_stream_empty_fields() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, colls) = CollectionWal::open(dir.path()).unwrap();

        // Stream entry with no fields
        let id = colls.xadd("empty", "1-0", vec![]).unwrap();
        drop(wal.log_xadd("empty", &id, &[]).unwrap());

        drop(wal);

        let (_wal2, colls2) = CollectionWal::open(dir.path()).unwrap();
        assert_eq!(colls2.xlen("empty").unwrap(), 1);
        let entries = colls2.xrange("empty", "-", "+", None).unwrap();
        assert!(entries[0].fields.is_empty());
    }

    #[test]
    fn test_stream_mixed_with_other_types() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, colls) = CollectionWal::open(dir.path()).unwrap();

        // Stream
        let id = colls
            .xadd("stream", "1-0", vec![("k".into(), "v".into())])
            .unwrap();
        drop(
            wal.log_xadd("stream", &id, &[("k".into(), "v".into())])
                .unwrap(),
        );

        // List
        colls.rpush("list", Value::Text("hello".into())).unwrap();
        drop(wal.log_rpush("list", &Value::Text("hello".into())).unwrap());

        // Set
        colls.sadd("set", "member").unwrap();
        drop(wal.log_sadd("set", "member").unwrap());

        drop(wal);

        let (_wal2, colls2) = CollectionWal::open(dir.path()).unwrap();
        assert_eq!(colls2.xlen("stream").unwrap(), 1);
        let items = colls2.lrange("list", 0, -1).unwrap();
        assert_eq!(items, vec![Value::Text("hello".into())]);
        assert!(colls2.sismember("set", "member").unwrap());
    }

    #[test]
    fn test_stream_checkpoint_with_all_types() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, colls) = CollectionWal::open(dir.path()).unwrap();

        // Create all types including stream
        colls.rpush("l", Value::Text("a".into())).unwrap();
        colls.hset("h", "f1", Value::Int32(1)).unwrap();
        colls.sadd("s", "x").unwrap();
        colls.zadd("z", "m1", 10.0).unwrap();
        colls.pfadd("p", "elem0").unwrap();
        colls
            .xadd(
                "stream",
                "100-0",
                vec![
                    ("sensor".into(), "temp".into()),
                    ("value".into(), "22.5".into()),
                ],
            )
            .unwrap();
        colls
            .xadd(
                "stream",
                "200-0",
                vec![
                    ("sensor".into(), "humidity".into()),
                    ("value".into(), "65".into()),
                ],
            )
            .unwrap();

        // Checkpoint all types
        wal.checkpoint(&colls).unwrap();
        drop(wal);

        // Reopen and verify everything including stream
        let (_wal2, colls2) = CollectionWal::open(dir.path()).unwrap();

        let list = colls2.lrange("l", 0, -1).unwrap();
        assert_eq!(list, vec![Value::Text("a".into())]);
        assert_eq!(colls2.hget("h", "f1").unwrap(), Some(Value::Int32(1)));
        assert!(colls2.sismember("s", "x").unwrap());
        assert_eq!(colls2.zcard("z").unwrap(), 1);
        assert!(colls2.pfcount("p").unwrap() >= 1);

        // Verify stream survived checkpoint
        assert_eq!(colls2.xlen("stream").unwrap(), 2);
        let entries = colls2.xrange("stream", "-", "+", None).unwrap();
        assert_eq!(entries[0].id, StreamId::new(100, 0));
        assert_eq!(
            entries[0].fields,
            vec![
                ("sensor".into(), "temp".into()),
                ("value".into(), "22.5".into()),
            ]
        );
        assert_eq!(entries[1].id, StreamId::new(200, 0));
        assert_eq!(
            entries[1].fields,
            vec![
                ("sensor".into(), "humidity".into()),
                ("value".into(), "65".into()),
            ]
        );
    }
}
