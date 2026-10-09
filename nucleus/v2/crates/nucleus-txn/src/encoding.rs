//! C-T0 §2.2/§2.3 on-disk encoding (draft 7.2): the intent/version/end keys
//! over prefix-free logical keys, the version and intent value formats, and
//! the persisted system keys.
//!
//! `L` is the caller's encoded logical key, used **as-is** (no length
//! prefix): by C-Q3s P-PREFIX it is order-preserving and prefix-free, so
//! logical keys keep their SQL order in the KV and no other logical key
//! starts with `L`. `L` cannot be delimited from the left without the
//! schema, so raw keys are split from the right (§2.2):
//!
//! ```text
//! intent        L ‖ 0x00
//! version @ts   L ‖ 0x01 ‖ be64(u64::MAX - ts) ‖ 0x01   # newest first
//! end(L)        L ‖ 0x02                                # exclusive upper bound
//! ```
//!
//! [`intent_key`], [`version_key`] and [`end_key`] agree byte-for-byte with
//! `nucleus_kv::conformance::layout` on logical keys made prefix-free by
//! `layout::logical` (its `be32` length prefix is a test convenience, not
//! part of the format; asserted by tests).

use crate::{Intent, Layer, LayerData, RowLockMode, Seq, Ts, TxnError, TxnId};
use nucleus_kv::{Key, Value};

// ---- Keys (§2.2) -----------------------------------------------------------

/// A version key is `L ‖ 0x01 ‖ be64(…) ‖ 0x01`: 10 bytes after `L`.
pub const VERSION_SUFFIX_LEN: usize = 10;

/// The `k@INTENT` slot (§1): at most one per logical key, sorts before every
/// version of `l`.
pub fn intent_key(l: &[u8]) -> Key {
    let mut k = Vec::with_capacity(l.len() + 1);
    k.extend_from_slice(l);
    k.push(0x00);
    k
}

/// The `k@ts` version key: versions of one logical key sort newest first
/// (descending ts). The trailing `0x01` tag lets [`parse_key`] find the
/// separator from the right without the schema (§2.2, draft 7.2).
pub fn version_key(l: &[u8], ts: Ts) -> Key {
    let mut k = Vec::with_capacity(l.len() + VERSION_SUFFIX_LEN);
    k.extend_from_slice(l);
    k.push(0x01);
    k.extend_from_slice(&(u64::MAX - ts.0).to_be_bytes());
    k.push(0x01);
    k
}

/// Exclusive upper bound of every entry (intent and versions) of `l`.
pub fn end_key(l: &[u8]) -> Key {
    let mut k = Vec::with_capacity(l.len() + 1);
    k.extend_from_slice(l);
    k.push(0x02);
    k
}

/// What [`parse_key`] found after the logical key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Entry {
    Intent,
    Version(Ts),
}

/// Splits a raw key from the right (§2.2): the last byte `0x00` is an intent
/// (`L` = all but the last byte); the last byte `0x01`, with the byte 9
/// positions before it also `0x01`, is a version (`L` = all but the last 10
/// bytes). `None` for anything else: `end_key`s and foreign keys.
pub fn parse_key(key: &[u8]) -> Option<(&[u8], Entry)> {
    match *key.last()? {
        0x00 => Some((&key[..key.len() - 1], Entry::Intent)),
        0x01 => {
            if key.len() < VERSION_SUFFIX_LEN || key[key.len() - VERSION_SUFFIX_LEN] != 0x01 {
                return None;
            }
            let mut b = [0u8; 8];
            b.copy_from_slice(&key[key.len() - 9..key.len() - 1]);
            Some((
                &key[..key.len() - VERSION_SUFFIX_LEN],
                Entry::Version(Ts(u64::MAX - u64::from_be_bytes(b))),
            ))
        }
        _ => None,
    }
}

// ---- Version values (§2.2) -------------------------------------------------

/// Version value headers: `header ‖ payload`; tombstones carry no payload.
pub const HEADER_LIVE: u8 = 0x00;
pub const HEADER_LIVE_KEY_CHANGED: u8 = 0x01;
pub const HEADER_TOMBSTONE: u8 = 0x02;
pub const HEADER_MOVED_TOMBSTONE: u8 = 0x03;

/// Encodes a layer's data as a version value. `Absent` produces no version
/// (§7.3 step 3), hence `None`.
pub fn encode_version(data: &LayerData) -> Option<Value> {
    match data {
        LayerData::Write { value, key_changed } => {
            let mut v = Vec::with_capacity(1 + value.len());
            v.push(if *key_changed {
                HEADER_LIVE_KEY_CHANGED
            } else {
                HEADER_LIVE
            });
            v.extend_from_slice(value);
            Some(v)
        }
        LayerData::Delete { moved } => Some(vec![if *moved {
            HEADER_MOVED_TOMBSTONE
        } else {
            HEADER_TOMBSTONE
        }]),
        LayerData::Absent => None,
    }
}

/// A decoded version value (§2.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionValue {
    Live { payload: Vec<u8>, key_changed: bool },
    Tombstone { moved: bool },
}

impl VersionValue {
    /// `None` = not-found for reads: both tombstone kinds (§4 step 2).
    pub fn into_option(self) -> Option<Vec<u8>> {
        match self {
            VersionValue::Live { payload, .. } => Some(payload),
            VersionValue::Tombstone { .. } => None,
        }
    }
}

/// Decodes a version value. Unknown headers, tombstones with a payload, or
/// truncated input are errors, never panics.
pub fn decode_version(value: &[u8]) -> Result<VersionValue, TxnError> {
    let corrupt = |what: &str| TxnError::Corrupt(format!("version value: {what}"));
    match value.first() {
        Some(&HEADER_LIVE) => Ok(VersionValue::Live {
            payload: value[1..].to_vec(),
            key_changed: false,
        }),
        Some(&HEADER_LIVE_KEY_CHANGED) => Ok(VersionValue::Live {
            payload: value[1..].to_vec(),
            key_changed: true,
        }),
        Some(&HEADER_TOMBSTONE) => {
            if value.len() != 1 {
                Err(corrupt("tombstone with payload"))
            } else {
                Ok(VersionValue::Tombstone { moved: false })
            }
        }
        Some(&HEADER_MOVED_TOMBSTONE) => {
            if value.len() != 1 {
                Err(corrupt("moved-tombstone with payload"))
            } else {
                Ok(VersionValue::Tombstone { moved: true })
            }
        }
        _ => Err(corrupt("bad header")),
    }
}

// ---- Intent values ---------------------------------------------------------

/// Intent value format tag. The value is versioned and self-describing so
/// unknown future formats are refused, not misread.
pub const INTENT_MAGIC: [u8; 4] = *b"NTXI";
pub const INTENT_FORMAT_VERSION: u8 = 1;

/// A layer is at least `seq(4) + data_seq(4) + data tag(1) + lock(1)` bytes;
/// used to bound the layer count before allocating.
const MIN_LAYER_LEN: usize = 10;

fn lock_byte(lock: RowLockMode) -> u8 {
    match lock {
        RowLockMode::KeyShare => 0,
        RowLockMode::Share => 1,
        RowLockMode::NoKeyUpdate => 2,
        RowLockMode::Update => 3,
    }
}

fn lock_from_byte(b: u8) -> Result<RowLockMode, TxnError> {
    match b {
        0 => Ok(RowLockMode::KeyShare),
        1 => Ok(RowLockMode::Share),
        2 => Ok(RowLockMode::NoKeyUpdate),
        3 => Ok(RowLockMode::Update),
        _ => Err(TxnError::Corrupt(format!("intent layer lock byte {b:#x}"))),
    }
}

fn bool_byte(v: bool) -> u8 {
    u8::from(v)
}

fn bool_from_byte(b: u8, what: &str) -> Result<bool, TxnError> {
    match b {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(TxnError::Corrupt(format!("intent {what} byte {b:#x}"))),
    }
}

/// Encodes an intent (§2.1): magic, format version, owner, layer count, then
/// each layer oldest first — `seq`, `data_seq`, `data`, `lock`. An intent
/// with no layers violates §2.1 ("never empty") and is an error.
pub fn encode_intent(intent: &Intent) -> Result<Value, TxnError> {
    if intent.layers.is_empty() {
        return Err(TxnError::Corrupt("intent with no layers".into()));
    }
    let mut v = Vec::with_capacity(6 + 12 + intent.layers.len() * MIN_LAYER_LEN);
    v.extend_from_slice(&INTENT_MAGIC);
    v.push(INTENT_FORMAT_VERSION);
    v.extend_from_slice(&intent.txn.epoch.to_be_bytes());
    v.extend_from_slice(&intent.txn.n.to_be_bytes());
    let n = u32::try_from(intent.layers.len())
        .map_err(|_| TxnError::Corrupt("intent layer count overflows u32".into()))?;
    v.extend_from_slice(&n.to_be_bytes());
    for layer in &intent.layers {
        let Layer {
            seq,
            data_seq,
            data,
            lock,
        } = layer;
        v.extend_from_slice(&seq.to_be_bytes());
        v.extend_from_slice(&data_seq.to_be_bytes());
        match data {
            LayerData::Absent => v.push(0x00),
            LayerData::Write { value, key_changed } => {
                v.push(0x01);
                let len = u32::try_from(value.len()).map_err(|_| {
                    TxnError::Corrupt("intent layer value length overflows u32".into())
                })?;
                v.extend_from_slice(&len.to_be_bytes());
                v.extend_from_slice(value);
                v.push(bool_byte(*key_changed));
            }
            LayerData::Delete { moved } => {
                v.push(0x02);
                v.push(bool_byte(*moved));
            }
        }
        v.push(lock_byte(*lock));
    }
    Ok(v)
}

/// Bounds-checked cursor: corrupt input is an error, never a panic.
struct Reader<'a> {
    b: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn u8(&mut self) -> Result<u8, TxnError> {
        let b = self.b.get(self.pos).copied();
        self.pos += 1;
        b.ok_or_else(|| TxnError::Corrupt("intent value truncated".into()))
    }
    fn u32(&mut self) -> Result<u32, TxnError> {
        let mut n = [0u8; 4];
        n.copy_from_slice(self.take(4)?);
        Ok(u32::from_be_bytes(n))
    }
    fn u64(&mut self) -> Result<u64, TxnError> {
        let mut n = [0u8; 8];
        n.copy_from_slice(self.take(8)?);
        Ok(u64::from_be_bytes(n))
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], TxnError> {
        let s = self
            .b
            .get(self.pos..self.pos.saturating_add(n))
            .ok_or_else(|| TxnError::Corrupt("intent value truncated".into()))?;
        self.pos += n;
        Ok(s)
    }
    fn done(&self) -> bool {
        self.pos == self.b.len()
    }
}

/// Decodes an intent written by [`encode_intent`]. Rejects bad magic, unknown
/// format versions, zero layers (§2.1), non-monotonic layer seqs, out-of-range
/// tags, and trailing bytes. Never panics on any input.
pub fn decode_intent(value: &[u8]) -> Result<Intent, TxnError> {
    let mut r = Reader { b: value, pos: 0 };
    if r.take(4)? != INTENT_MAGIC {
        return Err(TxnError::Corrupt("intent value: bad magic".into()));
    }
    let version = r.u8()?;
    if version != INTENT_FORMAT_VERSION {
        return Err(TxnError::Corrupt(format!(
            "intent value: format version {version} not supported"
        )));
    }
    let epoch = r.u32()?;
    let n = r.u64()?;
    let layer_count = r.u32()? as usize;
    if layer_count == 0 {
        return Err(TxnError::Corrupt("intent value: no layers".into()));
    }
    if r.b.len().saturating_sub(r.pos) < layer_count.saturating_mul(MIN_LAYER_LEN) {
        return Err(TxnError::Corrupt(
            "intent value: layer count too large".into(),
        ));
    }
    let mut layers: Vec<Layer> = Vec::with_capacity(layer_count);
    for i in 0..layer_count {
        let seq: Seq = r.u32()?;
        let data_seq: Seq = r.u32()?;
        if let Some(prev) = layers.last() {
            if seq <= prev.seq {
                return Err(TxnError::Corrupt(format!(
                    "intent layer {i}: seq {seq} not above previous {}",
                    prev.seq
                )));
            }
        }
        let data = match r.u8()? {
            0x00 => LayerData::Absent,
            0x01 => {
                let len = r.u32()? as usize;
                let value = r.take(len)?.to_vec();
                let key_changed = bool_from_byte(r.u8()?, "key_changed")?;
                LayerData::Write { value, key_changed }
            }
            0x02 => {
                let moved = bool_from_byte(r.u8()?, "moved")?;
                LayerData::Delete { moved }
            }
            tag => {
                return Err(TxnError::Corrupt(format!(
                    "intent layer {i}: data tag {tag:#x}"
                )))
            }
        };
        let lock = lock_from_byte(r.u8()?)?;
        layers.push(Layer {
            seq,
            data_seq,
            data,
            lock,
        });
    }
    if !r.done() {
        return Err(TxnError::Corrupt("intent value: trailing bytes".into()));
    }
    Ok(Intent {
        txn: TxnId { epoch, n },
        layers,
    })
}

// ---- Persisted system keys (§2.3) -----------------------------------------

/// System keys live under `/sys/` and sort outside every `/t/`, `/u/` and
/// `/i/` data prefix (`/i/` < `/s` < `/t/`, `/u/` in the first byte after
/// the slash).
pub const SYS_PREFIX: &[u8] = b"/sys/";

/// `/sys/txn/{TxnId}`: only `Committed(ts)` records are ever persisted (§2.3).
/// The id is `be32(epoch) ‖ be64(n)`, fixed width, so records sort by
/// `(epoch, n)`.
pub fn sys_txn_key(id: TxnId) -> Key {
    let mut k = SYS_PREFIX.to_vec();
    k.extend_from_slice(b"txn/");
    k.extend_from_slice(&id.epoch.to_be_bytes());
    k.extend_from_slice(&id.n.to_be_bytes());
    k
}

/// Splits a `/sys/txn/{TxnId}` key back into its id.
pub fn parse_sys_txn_key(key: &[u8]) -> Option<TxnId> {
    let tail = key.strip_prefix(SYS_PREFIX)?.strip_prefix(b"txn/")?;
    if tail.len() != 12 {
        return None;
    }
    let mut e = [0u8; 4];
    e.copy_from_slice(&tail[..4]);
    let mut n = [0u8; 8];
    n.copy_from_slice(&tail[4..]);
    Some(TxnId {
        epoch: u32::from_be_bytes(e),
        n: u64::from_be_bytes(n),
    })
}

/// The `/sys/txn/` prefix covering every status record.
pub fn sys_txn_prefix() -> Key {
    let mut k = SYS_PREFIX.to_vec();
    k.extend_from_slice(b"txn/");
    k
}

/// Exclusive upper bound of the `/sys/txn/` range: `/sys/txn0`. Together with
/// [`sys_txn_prefix`] this covers exactly the keys starting `/sys/txn/` (no
/// other `/sys/` key sorts inside it).
pub fn sys_txn_prefix_end() -> Key {
    let mut k = sys_txn_prefix();
    let last = k.len() - 1;
    k[last] = k[last].saturating_add(1);
    k
}

/// `/sys/epoch` (§2.3): u32, incremented and synced at boot.
pub fn sys_epoch_key() -> Key {
    let mut k = SYS_PREFIX.to_vec();
    k.extend_from_slice(b"epoch");
    k
}

/// `/sys/ts_hwm` (§2.3): sequencer high-water mark.
pub fn sys_ts_hwm_key() -> Key {
    let mut k = SYS_PREFIX.to_vec();
    k.extend_from_slice(b"ts_hwm");
    k
}

/// `/sys/gc_w` (§2.3): the GC watermark `W`, monotonic, synced.
pub fn sys_gc_w_key() -> Key {
    let mut k = SYS_PREFIX.to_vec();
    k.extend_from_slice(b"gc_w");
    k
}

/// `/sys/ts_clock/{ts}` (§2.3): sparse `(wall_time, ts)` samples; the ts is
/// the key so samples sort by it.
pub fn sys_ts_clock_key(ts: Ts) -> Key {
    let mut k = SYS_PREFIX.to_vec();
    k.extend_from_slice(b"ts_clock/");
    k.extend_from_slice(&ts.0.to_be_bytes());
    k
}
