//! §10 catalog rows and keys: the `/sys/catalog/` layout, the relation and
//! index row codecs, the persisted id-counter keys, and the storage-range
//! conventions the SSI storage map is rebuilt from. The rows themselves are
//! ordinary versioned rows (§2.2 layout over these logical keys); this
//! module only fixes their bytes.

use nucleus_kv::Key;

use crate::encoding::SYS_PREFIX;
use crate::TxnError;

// ---- keys (§10) -------------------------------------------------------------
//
// `/sys/catalog/` is the one `/sys/` subtree §10 makes versioned table
// data; the fixed-width be64 oid keeps `[prefix, prefix_end)` covering
// exactly one row family.

/// The catalog subtree prefix (`/sys/catalog/`, §10).
pub const SYS_CATALOG_PREFIX: &[u8] = b"/sys/catalog/";

/// `/sys/catalog/rel/`: one row per relation, keyed by `be64(oid)`.
pub fn rel_prefix() -> Key {
    let mut k = SYS_CATALOG_PREFIX.to_vec();
    k.extend_from_slice(b"rel/");
    k
}

/// Exclusive upper bound of [`rel_prefix`]: `/sys/catalog/rel0`.
pub fn rel_prefix_end() -> Key {
    prefix_end(rel_prefix())
}

/// The logical key of relation `oid`'s catalog row.
pub fn rel_key(oid: u64) -> Key {
    let mut k = rel_prefix();
    k.extend_from_slice(&oid.to_be_bytes());
    k
}

/// Splits a `/sys/catalog/rel/{be64 oid}` logical key back into its oid.
pub fn parse_rel_key(key: &[u8]) -> Option<u64> {
    parse_prefixed_be64(key, &rel_prefix())
}

/// `/sys/catalog/idx/`: one row per index, keyed by `be64(oid)`.
pub fn idx_prefix() -> Key {
    let mut k = SYS_CATALOG_PREFIX.to_vec();
    k.extend_from_slice(b"idx/");
    k
}

/// Exclusive upper bound of [`idx_prefix`]: `/sys/catalog/idx0`.
pub fn idx_prefix_end() -> Key {
    prefix_end(idx_prefix())
}

/// The logical key of index `oid`'s catalog row.
pub fn idx_key(oid: u64) -> Key {
    let mut k = idx_prefix();
    k.extend_from_slice(&oid.to_be_bytes());
    k
}

/// Splits a `/sys/catalog/idx/{be64 oid}` logical key back into its oid.
pub fn parse_idx_key(key: &[u8]) -> Option<u64> {
    parse_prefixed_be64(key, &idx_prefix())
}

/// `/sys/next_storage_id` (§10): the persisted storage-id counter.
pub fn next_storage_id_key() -> Key {
    sys_key(b"next_storage_id")
}

/// `/sys/next_oid` (§10): the persisted catalog-oid counter.
pub fn next_oid_key() -> Key {
    sys_key(b"next_oid")
}

fn sys_key(name: &[u8]) -> Key {
    let mut k = SYS_PREFIX.to_vec();
    k.extend_from_slice(name);
    k
}

/// Exclusive upper bound of a prefix whose last byte is below `0xFF`
/// (mirrors [`crate::encoding::sys_txn_prefix_end`]).
fn prefix_end(mut k: Key) -> Key {
    let last = k.len() - 1;
    k[last] = k[last].saturating_add(1);
    k
}

fn parse_prefixed_be64(key: &[u8], prefix: &[u8]) -> Option<u64> {
    let tail = key.strip_prefix(prefix)?;
    if tail.len() != 8 {
        return None;
    }
    let mut b = [0u8; 8];
    b.copy_from_slice(tail);
    Some(u64::from_be_bytes(b))
}

/// A `[lo, hi)` pair of logical keys.
pub type KeyRange = (Key, Key);

// ---- storage ranges ---------------------------------------------------------

/// The table storage range of `sid`: every logical key
/// `/t/{be64(sid)}/{pk}` (§1: `{rel}` in a key is a storage id, never an
/// oid). The be64 keeps the id fixed-width, so the range covers exactly one
/// storage. The bytes after the separator are the SQL codec's business
/// (C-Q3s/C-K1); the catalog owns only the prefix and separator
/// convention, which the executor cards must match.
pub fn table_range(sid: u64) -> (Key, Key) {
    data_range(b"/t/", sid)
}

/// The index entry ranges of index storage id `sid`: `/u/{sid}/…` (unique
/// entries) and `/i/{sid}/…` (non-unique and deferrable entries, §5.3).
/// Both map to the owning relation (§12 Q7), so both are the index's
/// storage.
pub fn index_ranges(sid: u64) -> Vec<(Key, Key)> {
    vec![data_range(b"/u/", sid), data_range(b"/i/", sid)]
}

fn data_range(prefix: &[u8], sid: u64) -> (Key, Key) {
    let mut lo = prefix.to_vec();
    lo.extend_from_slice(&sid.to_be_bytes());
    lo.push(b'/');
    let mut hi = lo.clone();
    let last = hi.len() - 1;
    hi[last] = hi[last].saturating_add(1); // '/' (0x2F) -> '0' (0x30)
    (lo, hi)
}

// ---- relation rows ----------------------------------------------------------

/// A relation's kind (§10 "kind"). 2.0 has tables only; the byte is
/// persisted, so new kinds are additive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelKind {
    Table,
}

impl RelKind {
    fn as_byte(self) -> u8 {
        match self {
            RelKind::Table => 0x01,
        }
    }

    fn from_byte(b: u8) -> Result<RelKind, TxnError> {
        match b {
            0x01 => Ok(RelKind::Table),
            other => Err(TxnError::Corrupt(format!(
                "catalog rel row: kind {other:#x}"
            ))),
        }
    }
}

/// One `/sys/catalog/rel/{oid}` row: the oid, its kind, its current table
/// storage id, the name and an opaque definition (the SQL layer's bytes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelRow {
    pub oid: u64,
    pub kind: RelKind,
    pub storage_id: u64,
    pub name: Vec<u8>,
    pub def: Vec<u8>,
}

impl RelRow {
    /// Self-describing: `tag ‖ be64(oid) ‖ kind ‖ be64(storage_id) ‖ name ‖
    /// def`, each byte string length-prefixed be32.
    pub fn encode(&self) -> Result<Vec<u8>, TxnError> {
        let mut v = Vec::with_capacity(18 + self.name.len() + self.def.len());
        v.push(0x01);
        v.extend_from_slice(&self.oid.to_be_bytes());
        v.push(self.kind.as_byte());
        v.extend_from_slice(&self.storage_id.to_be_bytes());
        put_bytes(&mut v, &self.name)?;
        put_bytes(&mut v, &self.def)?;
        Ok(v)
    }

    /// The same row with `storage_id` replaced (TRUNCATE's rewrite).
    pub fn with_storage(&self, storage_id: u64) -> RelRow {
        RelRow {
            storage_id,
            ..self.clone()
        }
    }

    pub fn decode(b: &[u8]) -> Result<RelRow, TxnError> {
        let mut r = Reader { b, pos: 0 };
        let tag = r.u8()?;
        if tag != 0x01 {
            return Err(TxnError::Corrupt(format!("catalog rel row: tag {tag:#x}")));
        }
        let oid = r.u64()?;
        let kind = RelKind::from_byte(r.u8()?)?;
        let storage_id = r.u64()?;
        let name = r.bytes()?;
        let def = r.bytes()?;
        r.done()?;
        Ok(RelRow {
            oid,
            kind,
            storage_id,
            name,
            def,
        })
    }
}

// ---- index rows -------------------------------------------------------------

/// Where an index's entries live.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdxStorage {
    /// A storage id: entries under `/u/{sid}/…` and `/i/{sid}/…`
    /// ([`index_ranges`]). TRUNCATE can retire and re-allocate it.
    Id(u64),
    /// An explicit entry range whose layout the caller owns (no storage id
    /// embeds the prefix). TRUNCATE leaves it in place: §9.2 retires
    /// storage **ids**, and a range without one has nothing to re-allocate.
    Range(Key, Key),
}

impl IdxStorage {
    /// Every range this index's entries occupy.
    pub fn ranges(&self) -> Vec<(Key, Key)> {
        match self {
            IdxStorage::Id(sid) => index_ranges(*sid),
            IdxStorage::Range(lo, hi) => vec![(lo.clone(), hi.clone())],
        }
    }

    fn encode(&self, v: &mut Vec<u8>) -> Result<(), TxnError> {
        match self {
            IdxStorage::Id(sid) => {
                v.push(0x01);
                v.extend_from_slice(&sid.to_be_bytes());
            }
            IdxStorage::Range(lo, hi) => {
                v.push(0x02);
                put_bytes(v, lo)?;
                put_bytes(v, hi)?;
            }
        }
        Ok(())
    }

    fn decode(r: &mut Reader<'_>) -> Result<IdxStorage, TxnError> {
        match r.u8()? {
            0x01 => Ok(IdxStorage::Id(r.u64()?)),
            0x02 => {
                let lo = r.bytes()?;
                let hi = r.bytes()?;
                Ok(IdxStorage::Range(lo, hi))
            }
            other => Err(TxnError::Corrupt(format!(
                "catalog idx row: storage tag {other:#x}"
            ))),
        }
    }
}

/// One `/sys/catalog/idx/{oid}` row: the owning relation's oid, the entry
/// storage, and an opaque definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdxRow {
    pub oid: u64,
    pub owning_rel: u64,
    pub storage: IdxStorage,
    pub def: Vec<u8>,
}

impl IdxRow {
    /// Self-describing: `tag ‖ be64(oid) ‖ be64(owning_rel) ‖ storage ‖ def`.
    pub fn encode(&self) -> Result<Vec<u8>, TxnError> {
        let mut v = Vec::with_capacity(17 + self.def.len());
        v.push(0x02);
        v.extend_from_slice(&self.oid.to_be_bytes());
        v.extend_from_slice(&self.owning_rel.to_be_bytes());
        self.storage.encode(&mut v)?;
        put_bytes(&mut v, &self.def)?;
        Ok(v)
    }

    /// The same row with the storage id replaced (TRUNCATE's rewrite of an
    /// [`IdxStorage::Id`] index).
    pub fn with_storage_id(&self, sid: u64) -> IdxRow {
        IdxRow {
            storage: IdxStorage::Id(sid),
            ..self.clone()
        }
    }

    pub fn decode(b: &[u8]) -> Result<IdxRow, TxnError> {
        let mut r = Reader { b, pos: 0 };
        let tag = r.u8()?;
        if tag != 0x02 {
            return Err(TxnError::Corrupt(format!("catalog idx row: tag {tag:#x}")));
        }
        let oid = r.u64()?;
        let owning_rel = r.u64()?;
        let storage = IdxStorage::decode(&mut r)?;
        let def = r.bytes()?;
        r.done()?;
        Ok(IdxRow {
            oid,
            owning_rel,
            storage,
            def,
        })
    }
}

// ---- codec helpers ----------------------------------------------------------

/// Appends `be32(len) ‖ bytes`; an over-long byte string is an error, never
/// a truncation.
fn put_bytes(v: &mut Vec<u8>, b: &[u8]) -> Result<(), TxnError> {
    let len = u32::try_from(b.len())
        .map_err(|_| TxnError::Corrupt("catalog row field over u32".into()))?;
    v.extend_from_slice(&len.to_be_bytes());
    v.extend_from_slice(b);
    Ok(())
}

/// Bounds-checked cursor; corrupt input is an error, never a panic.
struct Reader<'a> {
    b: &'a [u8],
    pos: usize,
}

impl Reader<'_> {
    fn u8(&mut self) -> Result<u8, TxnError> {
        let b = self.b.get(self.pos).copied();
        self.pos += 1;
        b.ok_or_else(|| TxnError::Corrupt("catalog row truncated".into()))
    }

    fn u64(&mut self) -> Result<u64, TxnError> {
        let mut n = [0u8; 8];
        n.copy_from_slice(self.take(8)?);
        Ok(u64::from_be_bytes(n))
    }

    fn bytes(&mut self) -> Result<Vec<u8>, TxnError> {
        let mut n = [0u8; 4];
        n.copy_from_slice(self.take(4)?);
        let len = u32::from_be_bytes(n) as usize;
        Ok(self.take(len)?.to_vec())
    }

    fn take(&mut self, n: usize) -> Result<&[u8], TxnError> {
        let s = self
            .b
            .get(self.pos..self.pos.saturating_add(n))
            .ok_or_else(|| TxnError::Corrupt("catalog row truncated".into()))?;
        self.pos += n;
        Ok(s)
    }

    fn done(&self) -> Result<(), TxnError> {
        if self.pos == self.b.len() {
            Ok(())
        } else {
            Err(TxnError::Corrupt("catalog row trailing bytes".into()))
        }
    }
}

#[cfg(test)]
mod tests {
    //! Codec round-trips and corrupt-input rejection (bounds and tags).
    //! Mutants: a field dropped by decode; trailing bytes accepted.

    fn ok<T, E: std::fmt::Debug>(r: Result<T, E>) -> T {
        match r {
            Ok(v) => v,
            Err(e) => panic!("unexpected error: {e:?}"),
        }
    }

    #[test]
    fn rel_and_idx_rows_round_trip() {
        let rel = super::RelRow {
            oid: 7,
            kind: super::RelKind::Table,
            storage_id: 42,
            name: b"accounts".to_vec(),
            def: b"def-bytes\x00with-nuls".to_vec(),
        };
        assert_eq!(ok(super::RelRow::decode(&ok(rel.encode()))), rel);
        let idx = super::IdxRow {
            oid: 9,
            owning_rel: 7,
            storage: super::IdxStorage::Id(43),
            def: Vec::new(),
        };
        assert_eq!(ok(super::IdxRow::decode(&ok(idx.encode()))), idx);
        let ranged = super::IdxRow {
            oid: 10,
            owning_rel: 7,
            storage: super::IdxStorage::Range(b"/i/x/".to_vec(), b"/i/x0".to_vec()),
            def: b"d".to_vec(),
        };
        assert_eq!(ok(super::IdxRow::decode(&ok(ranged.encode()))), ranged);
    }

    #[test]
    fn corrupt_rows_are_errors_never_panics() {
        assert!(super::RelRow::decode(&[]).is_err());
        assert!(super::RelRow::decode(b"\x02").is_err()); // wrong tag
        let mut good = ok(super::RelRow {
            oid: 1,
            kind: super::RelKind::Table,
            storage_id: 2,
            name: vec![],
            def: vec![],
        }
        .encode());
        good.push(0); // trailing byte
        assert!(super::RelRow::decode(&good).is_err());
        assert!(super::IdxRow::decode(&[0x02, 0x00]).is_err()); // truncated
    }

    #[test]
    fn key_helpers_split_and_bound() {
        let k = super::rel_key(5);
        assert_eq!(super::parse_rel_key(&k), Some(5));
        assert_eq!(super::parse_idx_key(&super::idx_key(6)), Some(6));
        assert_eq!(super::parse_rel_key(&super::idx_key(6)), None);
        // The prefix bounds cover exactly their rows.
        assert!(super::rel_key(u64::MAX).as_slice() < super::rel_prefix_end().as_slice());
        assert!(super::idx_prefix().as_slice() < super::rel_prefix().as_slice());
    }
}
