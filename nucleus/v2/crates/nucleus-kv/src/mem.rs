//! MemKv: `OrderedKv` over a persistent ordered map (V2 plan D1). For WASM,
//! tests and the G0 model.
//!
//! A write builds the next map version from the published one and publishes it
//! in one step, so batches are atomic. A snapshot is a clone of the published
//! map: O(1) by structural sharing. There is no WAL: `Durability` and
//! `sync_wal` are no-ops and a process exit loses everything not in a
//! checkpoint. Crash semantics are modelled by `fault::Fault`.

use std::fs::{self, File};
use std::io::{Read, Write};
use std::ops::Bound;
use std::path::Path;
use std::sync::{Mutex, PoisonError, RwLock};

use imbl::OrdMap;

use crate::{Batch, Durability, GcFilter, Key, KvError, Op, OrderedKv, Result, Snapshot, Value};

type Map = OrdMap<Key, Value>;

/// Checkpoint file format version written by `checkpoint`.
pub const CHECKPOINT_VERSION: u32 = 1;
const CHECKPOINT_FILE: &str = "MEMKV";
const CHECKPOINT_TMP: &str = "MEMKV.tmp";
const MAGIC: &[u8; 8] = b"NKVMEMCK";

pub struct MemKv {
    /// Published state. Readers clone it; only `publish` replaces it.
    map: RwLock<Map>,
    /// Serialises writers (and compaction) so each builds on the latest state.
    writer: Mutex<()>,
    gc: RwLock<Option<Box<dyn GcFilter>>>,
}

impl Default for MemKv {
    fn default() -> Self {
        Self::from_map(Map::new())
    }
}

impl MemKv {
    pub fn new() -> Self {
        Self::default()
    }

    fn from_map(map: Map) -> Self {
        Self {
            map: RwLock::new(map),
            writer: Mutex::new(()),
            gc: RwLock::new(None),
        }
    }

    fn current(&self) -> Map {
        self.map
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Applies `f` to a copy of the latest state and publishes it, or nothing on error.
    fn publish(&self, f: impl FnOnce(&mut Map) -> Result<()>) -> Result<()> {
        let _w = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
        let mut next = self.current();
        f(&mut next)?;
        *self.map.write().unwrap_or_else(PoisonError::into_inner) = next;
        Ok(())
    }

    /// Runs the registered GC filter over every key in ascending order as one
    /// compaction stream and removes the keys it drops, atomically. Returns the
    /// number dropped; 0 without a filter. Snapshots taken earlier keep seeing
    /// dropped keys. The filter must not call back into this store.
    pub fn compact(&self) -> usize {
        let gc = self.gc.read().unwrap_or_else(PoisonError::into_inner);
        let Some(filter) = gc.as_ref() else {
            return 0;
        };
        let mut dropped = 0;
        let res = self.publish(|map| {
            let mut stream = filter.begin();
            let doomed: Vec<Key> = map
                .iter()
                .filter(|(k, v)| stream.drop_key(k, v))
                .map(|(k, _)| k.clone())
                .collect();
            for k in &doomed {
                map.remove(k);
            }
            dropped = doomed.len();
            Ok(())
        });
        match res {
            Ok(()) => dropped,
            Err(_) => 0,
        }
    }

    /// Opens a checkpoint written by `checkpoint`. A damaged file is
    /// `KvError::Corruption`; an unknown format version is `KvError::Format`.
    pub fn open_checkpoint(dir: &Path) -> Result<Self> {
        let mut bytes = Vec::new();
        File::open(dir.join(CHECKPOINT_FILE))?.read_to_end(&mut bytes)?;
        Ok(Self::from_map(decode(&bytes)?))
    }
}

impl OrderedKv for MemKv {
    type Snap = MemSnap;

    fn write(&self, batch: Batch, _sync: Durability) -> Result<()> {
        self.publish(|map| {
            for op in batch.ops {
                apply(map, op);
            }
            Ok(())
        })
    }

    fn sync_wal(&self) -> Result<()> {
        Ok(())
    }

    fn snapshot(&self) -> MemSnap {
        MemSnap {
            map: self.current(),
        }
    }

    fn get_latest(&self, key: &[u8]) -> Result<Option<Value>> {
        Ok(self
            .map
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(key)
            .cloned())
    }

    /// Rejects keys that are not strictly ascending (`KvError::Backend`),
    /// leaving the store untouched.
    fn ingest_sorted(&self, entries: &mut dyn Iterator<Item = (Key, Value)>) -> Result<()> {
        self.publish(|map| {
            let mut prev: Option<Key> = None;
            for (k, v) in entries {
                if prev.as_ref().is_some_and(|p| k <= *p) {
                    return Err(KvError::Backend(
                        "ingest_sorted: keys not strictly ascending".into(),
                    ));
                }
                map.insert(k.clone(), v);
                prev = Some(k);
            }
            Ok(())
        })
    }

    /// Writes one file, `dir/MEMKV`: magic, version, count, length-prefixed
    /// entries, CRC-32C trailer. `dir` must not exist; its parent must.
    fn checkpoint(&self, dir: &Path) -> Result<()> {
        let bytes = encode(&self.current())?;
        fs::create_dir(dir)?;
        let tmp = dir.join(CHECKPOINT_TMP);
        let mut f = File::create(&tmp)?;
        f.write_all(&bytes)?;
        f.sync_all()?;
        drop(f);
        fs::rename(&tmp, dir.join(CHECKPOINT_FILE))?;
        sync_dir(dir)
    }

    fn set_gc_filter(&self, filter: Box<dyn GcFilter>) {
        *self.gc.write().unwrap_or_else(PoisonError::into_inner) = Some(filter);
    }

    /// No native timestamps: GC runs through the filter in `compact` only.
    fn set_gc_watermark(&self, _watermark: u64) {}
}

/// Point-in-time view: a structurally shared copy of the map.
pub struct MemSnap {
    map: Map,
}

impl Snapshot for MemSnap {
    fn get(&self, key: &[u8]) -> Result<Option<Value>> {
        Ok(self.map.get(key).cloned())
    }

    fn scan<'a>(
        &'a self,
        range: (Bound<&[u8]>, Bound<&[u8]>),
        reverse: bool,
    ) -> Box<dyn Iterator<Item = Result<(Key, Value)>> + 'a> {
        if range_is_empty(range.0, range.1) {
            return Box::new(std::iter::empty());
        }
        let it = self
            .map
            .range::<_, [u8]>(range)
            .map(|(k, v)| Ok((k.clone(), v.clone())));
        if reverse {
            Box::new(it.rev())
        } else {
            Box::new(it)
        }
    }
}

fn apply(map: &mut Map, op: Op) {
    match op {
        Op::Put(k, v) => {
            map.insert(k, v);
        }
        Op::Delete(k) => {
            map.remove(&k);
        }
        Op::DeleteRange { start, end } => {
            let range = (
                Bound::Included(start.as_slice()),
                Bound::Excluded(end.as_slice()),
            );
            if range_is_empty(range.0, range.1) {
                return;
            }
            let doomed: Vec<Key> = map
                .range::<_, [u8]>(range)
                .map(|(k, _)| k.clone())
                .collect();
            for k in &doomed {
                map.remove(k);
            }
        }
    }
}

/// True for ranges that contain no key, including inverted ones.
fn range_is_empty(lo: Bound<&[u8]>, hi: Bound<&[u8]>) -> bool {
    match (lo, hi) {
        (Bound::Included(a), Bound::Included(b)) => a > b,
        (Bound::Included(a) | Bound::Excluded(a), Bound::Excluded(b))
        | (Bound::Excluded(a), Bound::Included(b)) => a >= b,
        _ => false,
    }
}

fn encode(map: &Map) -> Result<Vec<u8>> {
    let mut b = Vec::new();
    b.extend_from_slice(MAGIC);
    b.extend_from_slice(&CHECKPOINT_VERSION.to_le_bytes());
    b.extend_from_slice(&(map.len() as u64).to_le_bytes());
    for (k, v) in map.iter() {
        put_bytes(&mut b, k)?;
        put_bytes(&mut b, v)?;
    }
    let crc = crc32c(&b);
    b.extend_from_slice(&crc.to_le_bytes());
    Ok(b)
}

fn put_bytes(b: &mut Vec<u8>, s: &[u8]) -> Result<()> {
    let n = u32::try_from(s.len())
        .map_err(|_| KvError::Backend("checkpoint: entry longer than u32::MAX".into()))?;
    b.extend_from_slice(&n.to_le_bytes());
    b.extend_from_slice(s);
    Ok(())
}

fn corrupt(what: &str) -> KvError {
    KvError::Corruption(format!("memkv checkpoint: {what}"))
}

fn decode(b: &[u8]) -> Result<Map> {
    let mut head = Cursor { b, pos: 0 };
    if head.take(MAGIC.len())? != MAGIC.as_slice() {
        return Err(corrupt("bad magic"));
    }
    let found = head.u32()?;
    if found != CHECKPOINT_VERSION {
        return Err(KvError::Format {
            found,
            supported: CHECKPOINT_VERSION,
        });
    }
    let body_len = b
        .len()
        .checked_sub(4)
        .filter(|&n| n >= head.pos)
        .ok_or_else(|| corrupt("truncated"))?;
    let (body, trailer) = b.split_at(body_len);
    let mut want = [0u8; 4];
    want.copy_from_slice(trailer);
    if crc32c(body) != u32::from_le_bytes(want) {
        return Err(corrupt("checksum mismatch"));
    }
    let mut c = Cursor {
        b: body,
        pos: head.pos,
    };
    let count = c.u64()?;
    let mut map = Map::new();
    let mut prev: Option<&[u8]> = None;
    for _ in 0..count {
        let klen = c.u32()? as usize;
        let k = c.take(klen)?;
        let vlen = c.u32()? as usize;
        let v = c.take(vlen)?;
        if prev.is_some_and(|p| p >= k) {
            return Err(corrupt("keys out of order"));
        }
        map.insert(k.to_vec(), v.to_vec());
        prev = Some(k);
    }
    if c.pos != body.len() {
        return Err(corrupt("trailing bytes"));
    }
    Ok(map)
}

struct Cursor<'a> {
    b: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|&e| e <= self.b.len())
            .ok_or_else(|| corrupt("truncated"))?;
        let s = &self.b[self.pos..end];
        self.pos = end;
        Ok(s)
    }

    fn u32(&mut self) -> Result<u32> {
        let mut a = [0u8; 4];
        a.copy_from_slice(self.take(4)?);
        Ok(u32::from_le_bytes(a))
    }

    fn u64(&mut self) -> Result<u64> {
        let mut a = [0u8; 8];
        a.copy_from_slice(self.take(8)?);
        Ok(u64::from_le_bytes(a))
    }
}

#[cfg(unix)]
fn sync_dir(dir: &Path) -> Result<()> {
    File::open(dir)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_dir(_dir: &Path) -> Result<()> {
    Ok(())
}

const CRC32C_TABLE: [u32; 256] = crc32c_table();

const fn crc32c_table() -> [u32; 256] {
    let mut t = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 != 0 {
                0x82F6_3B78 ^ (c >> 1)
            } else {
                c >> 1
            };
            k += 1;
        }
        t[i] = c;
        i += 1;
    }
    t
}

/// CRC-32C (Castagnoli).
fn crc32c(data: &[u8]) -> u32 {
    let mut c = !0u32;
    for &b in data {
        c = CRC32C_TABLE[((c ^ u32::from(b)) & 0xff) as usize] ^ (c >> 8);
    }
    !c
}

/// Conformance harness for MemKv.
#[cfg(any(test, feature = "conformance"))]
#[derive(Debug, Clone, Copy, Default)]
pub struct MemHarness;

#[cfg(any(test, feature = "conformance"))]
impl crate::conformance::Harness for MemHarness {
    type Kv = MemKv;

    fn make(&self) -> MemKv {
        MemKv::new()
    }

    fn compact(&self, kv: &MemKv) -> Result<()> {
        kv.compact();
        Ok(())
    }

    fn open_checkpoint(&self, dir: &Path) -> Result<MemKv> {
        MemKv::open_checkpoint(dir)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conformance::{scratch_dir, FaultHarness};

    crate::kv_conformance_tests!(mem, MemHarness);
    crate::kv_conformance_tests!(fault_mem, FaultHarness(MemHarness));

    #[test]
    fn crc32c_check_value() {
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(crc32c(b""), 0);
    }

    fn sample() -> MemKv {
        let kv = MemKv::new();
        let mut b = Batch::default();
        b.put(vec![0], vec![1, 2, 3]);
        b.put(b"key".to_vec(), b"value".to_vec());
        b.put(vec![0xff, 0xff], Vec::new());
        assert!(kv.write(b, Durability::Yes).is_ok());
        kv
    }

    fn checkpoint_bytes() -> Vec<u8> {
        match encode(&sample().current()) {
            Ok(b) => b,
            Err(e) => panic!("encode: {e}"),
        }
    }

    #[test]
    fn decode_round_trips() {
        let map = sample().current();
        assert!(matches!(decode(&checkpoint_bytes()), Ok(m) if m == map));
    }

    #[test]
    fn every_flipped_byte_is_detected() {
        let good = checkpoint_bytes();
        for i in 0..good.len() {
            let mut bad = good.clone();
            bad[i] ^= 0x01;
            match decode(&bad) {
                Err(KvError::Corruption(_)) => {}
                // A flipped version field reads as an unknown version.
                Err(KvError::Format { .. }) if (8..12).contains(&i) => {}
                other => panic!("byte {i}: {:?}", other.map(|m| m.len())),
            }
        }
    }

    #[test]
    fn every_truncation_is_detected() {
        let good = checkpoint_bytes();
        for n in 0..good.len() {
            assert!(
                matches!(decode(&good[..n]), Err(KvError::Corruption(_))),
                "len {n}"
            );
        }
    }

    #[test]
    fn unknown_version_is_format_error() {
        let mut b = checkpoint_bytes();
        b[8..12].copy_from_slice(&2u32.to_le_bytes());
        assert!(matches!(
            decode(&b),
            Err(KvError::Format {
                found: 2,
                supported: CHECKPOINT_VERSION
            })
        ));
    }

    #[test]
    fn unsorted_body_with_valid_crc_is_corruption() {
        let mut b = Vec::new();
        b.extend_from_slice(MAGIC);
        b.extend_from_slice(&CHECKPOINT_VERSION.to_le_bytes());
        b.extend_from_slice(&2u64.to_le_bytes());
        for k in [b"b", b"a"] {
            assert!(put_bytes(&mut b, k).is_ok());
            assert!(put_bytes(&mut b, b"v").is_ok());
        }
        let crc = crc32c(&b);
        b.extend_from_slice(&crc.to_le_bytes());
        assert!(matches!(decode(&b), Err(KvError::Corruption(_))));
    }

    #[test]
    fn checkpoint_refuses_existing_dir_and_reports_damage() {
        let dir = scratch_dir("memkv-damage");
        let kv = sample();
        assert!(kv.checkpoint(&dir).is_ok());
        assert!(kv.checkpoint(&dir).is_err());
        let path = dir.join(CHECKPOINT_FILE);
        let mut bytes = match fs::read(&path) {
            Ok(b) => b,
            Err(e) => panic!("read: {e}"),
        };
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0x80;
        assert!(fs::write(&path, &bytes).is_ok());
        assert!(matches!(
            MemKv::open_checkpoint(&dir),
            Err(KvError::Corruption(_))
        ));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn compact_without_filter_drops_nothing() {
        assert_eq!(sample().compact(), 0);
    }
}
