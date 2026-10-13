//! Index definitions (C-T6 §6): the payload of `IdxRow.def`.
//!
//! ```text
//! def   = 0x49 0x01 flags(1) state(1) be64(built_at) be16(ncols)
//!         (be16(attnum) dir(1) nulls(1))*
//! flags = bit0 unique, bit1 nulls_not_distinct, bit2 concurrently
//! state = 0x00 DeleteOnly, 0x01 WriteOnly, 0x02 Public
//! ```
//!
//! The `dir` and `nulls` bytes are pinned here (`0x00`/`0x01` each) so the
//! on-disk numbers never derive from an enum's discriminant order, like the
//! §3.1 type tags. Decode is total: an unknown magic, flag bit, state, dir
//! or nulls byte, truncation and trailing bytes are
//! [`RowError::Corrupt`](nucleus_codec::row::RowError::Corrupt) (XX001),
//! never a panic. A change to these bytes is a format change: golden corpus
//! update and a format version bump (C-K4).

use nucleus_codec::row::RowError;
use nucleus_codec::{Direction, Nulls};

const DEF_MAGIC: [u8; 2] = [0x49, 0x01];

const FLAG_UNIQUE: u8 = 1 << 0;
const FLAG_NULLS_NOT_DISTINCT: u8 = 1 << 1;
const FLAG_CONCURRENTLY: u8 = 1 << 2;
const FLAG_KNOWN: u8 = FLAG_UNIQUE | FLAG_NULLS_NOT_DISTINCT | FLAG_CONCURRENTLY;

const DIR_ASC: u8 = 0x00;
const DIR_DESC: u8 = 0x01;
const NULLS_FIRST: u8 = 0x00;
const NULLS_LAST: u8 = 0x01;

const STATE_DELETE_ONLY: u8 = 0x00;
const STATE_WRITE_ONLY: u8 = 0x01;
const STATE_PUBLIC: u8 = 0x02;

/// One indexed column (§6): the attnum, its sort direction and where NULLs
/// sort inside it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexColumn {
    /// 1-based attnum of the indexed column in the owning relation's def.
    pub attnum: u16,
    pub dir: Direction,
    pub nulls: Nulls,
}

/// An index's state (§6): the online build's ladder (C-T6 §7). The bytes
/// are pinned above; new states are additive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexState {
    /// A delete (or an entry-changing update) deletes the old entry;
    /// nothing is inserted.
    DeleteOnly,
    /// Full maintenance, not yet readable.
    WriteOnly,
    /// Serves reads at snapshots `>= built_at` (§4).
    Public,
}

impl IndexState {
    fn as_byte(self) -> u8 {
        match self {
            IndexState::DeleteOnly => STATE_DELETE_ONLY,
            IndexState::WriteOnly => STATE_WRITE_ONLY,
            IndexState::Public => STATE_PUBLIC,
        }
    }

    fn from_byte(b: u8) -> Result<IndexState, RowError> {
        match b {
            STATE_DELETE_ONLY => Ok(IndexState::DeleteOnly),
            STATE_WRITE_ONLY => Ok(IndexState::WriteOnly),
            STATE_PUBLIC => Ok(IndexState::Public),
            _ => Err(RowError::Corrupt("unknown index state byte")),
        }
    }
}

fn dir_byte(d: Direction) -> u8 {
    match d {
        Direction::Asc => DIR_ASC,
        Direction::Desc => DIR_DESC,
    }
}

fn dir_from_byte(b: u8) -> Result<Direction, RowError> {
    match b {
        DIR_ASC => Ok(Direction::Asc),
        DIR_DESC => Ok(Direction::Desc),
        _ => Err(RowError::Corrupt("unknown index dir byte")),
    }
}

fn nulls_byte(n: Nulls) -> u8 {
    match n {
        Nulls::First => NULLS_FIRST,
        Nulls::Last => NULLS_LAST,
    }
}

fn nulls_from_byte(b: u8) -> Result<Nulls, RowError> {
    match b {
        NULLS_FIRST => Ok(Nulls::First),
        NULLS_LAST => Ok(Nulls::Last),
        _ => Err(RowError::Corrupt("unknown index nulls byte")),
    }
}

/// One index definition (§6): the `IdxRow.def` payload. `built_at` is the
/// ts the index became `Public` at (0 for a born-public index of C-T6
/// §6.1 case (a)); an index serves a read at `S` iff `Public` and
/// `built_at <= S`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexDef {
    /// The indexed columns, in key order.
    pub columns: Vec<IndexColumn>,
    pub unique: bool,
    /// `NULLS NOT DISTINCT`: NULL keys are checked like any value (§5.3).
    pub nulls_not_distinct: bool,
    /// `CREATE INDEX CONCURRENTLY` (an unfinished build marker, §7).
    pub concurrently: bool,
    pub state: IndexState,
    pub built_at: u64,
}

impl IndexDef {
    /// Serialises the definition (§6 bytes).
    pub fn encode(&self) -> Result<Vec<u8>, RowError> {
        let ncols = u16::try_from(self.columns.len())
            .map_err(|_| RowError::InvalidDdl("too many index columns"))?;
        let mut out = Vec::with_capacity(12 + self.columns.len() * 4);
        out.extend_from_slice(&DEF_MAGIC);
        let mut flags = 0u8;
        if self.unique {
            flags |= FLAG_UNIQUE;
        }
        if self.nulls_not_distinct {
            flags |= FLAG_NULLS_NOT_DISTINCT;
        }
        if self.concurrently {
            flags |= FLAG_CONCURRENTLY;
        }
        out.push(flags);
        out.push(self.state.as_byte());
        out.extend_from_slice(&self.built_at.to_be_bytes());
        out.extend_from_slice(&ncols.to_be_bytes());
        for col in &self.columns {
            out.extend_from_slice(&col.attnum.to_be_bytes());
            out.push(dir_byte(col.dir));
            out.push(nulls_byte(col.nulls));
        }
        Ok(out)
    }

    /// Parses a definition (§6 bytes). Total: every malformed input is an
    /// `Err`, never a panic.
    pub fn decode(bytes: &[u8]) -> Result<IndexDef, RowError> {
        let mut r = Reader { b: bytes, pos: 0 };
        let magic = r.take_array::<2>()?;
        if magic != DEF_MAGIC {
            return Err(RowError::Corrupt("index definition magic"));
        }
        let flags = r.u8()?;
        if flags & !FLAG_KNOWN != 0 {
            return Err(RowError::Corrupt("unknown index definition flags"));
        }
        let state = IndexState::from_byte(r.u8()?)?;
        let mut built_at = [0u8; 8];
        built_at.copy_from_slice(r.take(8)?);
        let ncols = usize::from(r.u16be()?);
        let mut columns = Vec::with_capacity(ncols);
        for _ in 0..ncols {
            let attnum = r.u16be()?;
            let dir = dir_from_byte(r.u8()?)?;
            let nulls = nulls_from_byte(r.u8()?)?;
            columns.push(IndexColumn { attnum, dir, nulls });
        }
        if r.pos != bytes.len() {
            return Err(RowError::Corrupt("trailing bytes after index definition"));
        }
        Ok(IndexDef {
            columns,
            unique: flags & FLAG_UNIQUE != 0,
            nulls_not_distinct: flags & FLAG_NULLS_NOT_DISTINCT != 0,
            concurrently: flags & FLAG_CONCURRENTLY != 0,
            state,
            built_at: u64::from_be_bytes(built_at),
        })
    }
}

/// Bounds-checked cursor; corrupt input is an error, never a panic (the
/// `rows.rs` pattern).
struct Reader<'a> {
    b: &'a [u8],
    pos: usize,
}

impl Reader<'_> {
    fn u8(&mut self) -> Result<u8, RowError> {
        let &b = self
            .b
            .get(self.pos)
            .ok_or(RowError::Corrupt("truncated index definition"))?;
        self.pos += 1;
        Ok(b)
    }

    fn u16be(&mut self) -> Result<u16, RowError> {
        Ok(u16::from_be_bytes(self.take_array::<2>()?))
    }

    fn take(&mut self, n: usize) -> Result<&[u8], RowError> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or(RowError::Corrupt("truncated index definition"))?;
        let s = self
            .b
            .get(self.pos..end)
            .ok_or(RowError::Corrupt("truncated index definition"))?;
        self.pos = end;
        Ok(s)
    }

    fn take_array<const N: usize>(&mut self) -> Result<[u8; N], RowError> {
        let s = self.take(N)?;
        s.try_into()
            .map_err(|_| RowError::Corrupt("truncated index definition"))
    }
}

#[cfg(test)]
mod tests {
    //! Round-trips and corrupt-input rejection (§6 bytes). Mutants: a
    //! field dropped by decode; a flag or state byte misread.

    use super::{IndexColumn, IndexDef, IndexState};

    fn ok<T, E: std::fmt::Debug>(r: Result<T, E>) -> T {
        match r {
            Ok(v) => v,
            Err(e) => panic!("unexpected error: {e:?}"),
        }
    }

    fn sample() -> IndexDef {
        IndexDef {
            columns: vec![
                IndexColumn {
                    attnum: 2,
                    dir: nucleus_codec::Direction::Asc,
                    nulls: nucleus_codec::Nulls::Last,
                },
                IndexColumn {
                    attnum: 5,
                    dir: nucleus_codec::Direction::Desc,
                    nulls: nucleus_codec::Nulls::First,
                },
            ],
            unique: true,
            nulls_not_distinct: true,
            concurrently: true,
            state: IndexState::WriteOnly,
            built_at: 0x0102_0304_0506_0708,
        }
    }

    #[test]
    fn round_trips_every_flag_and_state() {
        for &state in &[
            IndexState::DeleteOnly,
            IndexState::WriteOnly,
            IndexState::Public,
        ] {
            for &unique in &[false, true] {
                for &nnd in &[false, true] {
                    for &conc in &[false, true] {
                        let def = IndexDef {
                            state,
                            unique,
                            nulls_not_distinct: nnd,
                            concurrently: conc,
                            ..sample()
                        };
                        let bytes = ok(def.encode());
                        assert_eq!(ok(IndexDef::decode(&bytes)), def);
                    }
                }
            }
        }
        // No columns, born-public (§6.1 case (a)): built_at 0.
        let empty = IndexDef {
            columns: Vec::new(),
            unique: false,
            nulls_not_distinct: false,
            concurrently: false,
            state: IndexState::Public,
            built_at: 0,
        };
        assert_eq!(ok(IndexDef::decode(&ok(empty.encode()))), empty);
        // A column with every dir/nulls combination.
        for &dir in &[
            nucleus_codec::Direction::Asc,
            nucleus_codec::Direction::Desc,
        ] {
            for &nulls in &[nucleus_codec::Nulls::First, nucleus_codec::Nulls::Last] {
                let def = IndexDef {
                    columns: vec![IndexColumn {
                        attnum: 1,
                        dir,
                        nulls,
                    }],
                    ..sample()
                };
                assert_eq!(ok(IndexDef::decode(&ok(def.encode()))), def);
            }
        }
    }

    #[test]
    fn corrupt_definitions_are_errors_never_panics() {
        let good = ok(sample().encode());
        // Empty, short, wrong magic.
        assert!(IndexDef::decode(&[]).is_err());
        assert!(IndexDef::decode(&good[..good.len() - 1]).is_err());
        let mut bad_magic = good.clone();
        bad_magic[0] = 0x48;
        assert!(IndexDef::decode(&bad_magic).is_err());
        // Unknown flag bit (bit3), unknown state, unknown dir, unknown
        // nulls — each planted at its byte.
        let mut bad_flags = good.clone();
        bad_flags[2] |= 0x08;
        assert!(IndexDef::decode(&bad_flags).is_err());
        for state in [0x03u8, 0xFF] {
            let mut bad_state = good.clone();
            bad_state[3] = state;
            assert!(IndexDef::decode(&bad_state).is_err());
        }
        for byte in [0x02u8, 0xFF] {
            let mut bad_dir = good.clone();
            bad_dir[16] = byte; // first column's dir: 14 header + be16 attnum
            assert!(IndexDef::decode(&bad_dir).is_err());
            let mut bad_nulls = good.clone();
            bad_nulls[17] = byte;
            assert!(IndexDef::decode(&bad_nulls).is_err());
        }
        // Trailing bytes.
        let mut trailing = good;
        trailing.push(0);
        assert!(IndexDef::decode(&trailing).is_err());
        // A column count that outruns the input.
        let mut bomb = ok(sample().encode());
        bomb[12] = 0xFF; // ncols high byte
        bomb[13] = 0xFF; // ncols low byte
        assert!(IndexDef::decode(&bomb).is_err());
    }

    #[test]
    fn flags_and_state_bytes_are_pinned() {
        // The §6 bytes are constants, not enum discriminants: a Public,
        // unique, nulls-not-distinct, CONCURRENTLY def of no columns at
        // built_at 1 encodes to exactly these bytes.
        let def = IndexDef {
            columns: Vec::new(),
            unique: true,
            nulls_not_distinct: true,
            concurrently: true,
            state: IndexState::Public,
            built_at: 1,
        };
        assert_eq!(
            ok(def.encode()),
            vec![0x49, 0x01, 0x07, 0x02, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0]
        );
    }
}
