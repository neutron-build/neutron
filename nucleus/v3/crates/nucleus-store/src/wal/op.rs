//! The six ops (S-STORE §12.3; card A-6a, Work 3): [`Op`], its byte
//! encoding as card A-6a pins it under §12.3's "Card A-6a pins the op
//! encoding", and the order rule.
//!
//! Invariants this file owns (the module doc has the table):
//! - each op is a tag byte, then its fields in the spec's order,
//!   little-endian; a byte string is a `u32` length then its bytes; an
//!   optional `before` is a presence byte (0 absent, 1 present) then, if
//!   present, a byte string;
//! - a Put's flags use bit 0 (`key_changed`) and a Delete's bits 0
//!   (`moved`) and 1 (`revert`), after §7.3's provisional flags; every
//!   other bit is 0, so every op has exactly one encoding;
//! - the order rule, one implementation ([`OpOrder`]) for the encoder
//!   (`Invalid`) and the decoder (`Corrupt`): at most one `RequireCkpt`,
//!   which is then op 0 ("first in its record, before CreateTree, DropTree
//!   and Ddl too"); then `CreateTree`, `DropTree` and `Ddl` in any order
//!   ("first within a record but for a RequireCkpt"); then `Put` and
//!   `Delete`; and a `Delete` with `revert` only in a Chunk ("revert (Chunk
//!   only)");
//! - the decoder reads ops until the payload ends, every op wholly inside
//!   it, with every read bounded.

use crate::codec::{Reader, Writer};
use crate::store::StoreError;

/// `CreateTree`'s tag.
const TAG_CREATE_TREE: u8 = 1;
/// `DropTree`'s tag.
const TAG_DROP_TREE: u8 = 2;
/// `Ddl`'s tag.
const TAG_DDL: u8 = 3;
/// `Put`'s tag.
const TAG_PUT: u8 = 4;
/// `Delete`'s tag.
const TAG_DELETE: u8 = 5;
/// `RequireCkpt`'s tag.
const TAG_REQUIRE_CKPT: u8 = 6;

/// A Put's flag bit 0: `key_changed` (§7.3's tag 6 flags).
const PUT_KEY_CHANGED: u8 = 0b01;
/// A Delete's flag bit 0: `moved` (§7.3's tag 7 flags).
const DELETE_MOVED: u8 = 0b01;
/// A Delete's flag bit 1: `revert` (§12.3).
const DELETE_REVERT: u8 = 0b10;

/// A `before` presence byte: absent.
const BEFORE_ABSENT: u8 = 0;
/// A `before` presence byte: present, a byte string follows.
const BEFORE_PRESENT: u8 = 1;

/// One op of a Commit or a Chunk (S-STORE §12.3), its byte strings
/// borrowed from the record so a decode copies nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Op<'a> {
    /// `CreateTree{tree_id, kind}` (§12.3).
    CreateTree {
        /// The new tree's id.
        tree_id: u64,
        /// The tree's kind, a catalog value carried as it is.
        kind: u8,
    },
    /// `DropTree{tree_id, w u64}` (§12.3): `w` is the watermark the
    /// drop-cleanup job read, to which the DropTree's mark raises
    /// `gc_w_used`, in normal operation and in replay alike (§10.5
    /// "Drops", I-GCW).
    DropTree {
        /// The dropped tree's id.
        tree_id: u64,
        /// The watermark the drop-cleanup job read (§10.5).
        w: u64,
    },
    /// `Ddl{bytes}` (§12.3): catalog-owned bytes, opaque here.
    Ddl {
        /// The catalog's bytes.
        bytes: &'a [u8],
    },
    /// `Put{tree_id, key, flags, value, before?}` (§12.3; flags:
    /// `key_changed`).
    Put {
        /// The tree's id.
        tree_id: u64,
        /// The user key, not the entry key (§12.3, "Keys").
        key: &'a [u8],
        /// Flag bit 0: the write changed the row's key (§7.3).
        key_changed: bool,
        /// The value.
        value: &'a [u8],
        /// N14's optional before-image (§12.3).
        before: Option<&'a [u8]>,
    },
    /// `Delete{tree_id, key, flags, before?}` (§12.3; flags: `moved |
    /// revert (Chunk only)`).
    Delete {
        /// The tree's id.
        tree_id: u64,
        /// The user key.
        key: &'a [u8],
        /// Flag bit 0: an UPDATE moved the row to a new primary key (§7.3).
        moved: bool,
        /// Flag bit 1, only in a Chunk: the transaction no longer writes
        /// `key`, cancelling its earlier ops on it (§12.3, §12.7).
        revert: bool,
        /// N14's optional before-image (§12.3).
        before: Option<&'a [u8]>,
    },
    /// `RequireCkpt{ckpt_id}` (§12.3, §12.8, I-CKPT-DEP): the checkpoint a
    /// commit depends on; first in its record.
    RequireCkpt {
        /// The `ckpt_id` the forced checkpoint returned (§12.8).
        ckpt_id: u64,
    },
}

impl Op<'_> {
    /// The op's name in §12.3, for error messages.
    fn name(&self) -> &'static str {
        match self {
            Op::CreateTree { .. } => "CreateTree",
            Op::DropTree { .. } => "DropTree",
            Op::Ddl { .. } => "Ddl",
            Op::Put { .. } => "Put",
            Op::Delete { .. } => "Delete",
            Op::RequireCkpt { .. } => "RequireCkpt",
        }
    }
}

/// The order rule and the `revert` rule over a record's ops, admitted one
/// at a time (§12.3; card A-6a, Work 3): the encoder runs it over the list
/// it is handed, the decoder over the ops as it reads them.
pub(crate) struct OpOrder {
    /// Whether the ops are a Chunk's, where `revert` is allowed.
    in_chunk: bool,
    /// The index of the next op.
    next: usize,
    /// Whether a `Put` or `Delete` has been admitted.
    data_seen: bool,
}

impl OpOrder {
    /// The rule at the start of a Commit's (`in_chunk` false) or a Chunk's
    /// (`in_chunk` true) ops.
    pub(crate) fn new(in_chunk: bool) -> Self {
        Self {
            in_chunk,
            next: 0,
            data_seen: false,
        }
    }

    /// Admits `op` as the next op, or gives the reason it may not come
    /// next.
    pub(crate) fn admit(&mut self, op: &Op<'_>) -> Result<(), String> {
        let i = self.next;
        match op {
            Op::RequireCkpt { .. } if i != 0 => {
                return Err(format!(
                    "RequireCkpt is op {i}; it is first in its record (S-STORE §12.3)"
                ));
            }
            Op::RequireCkpt { .. } => {}
            Op::CreateTree { .. } | Op::DropTree { .. } | Op::Ddl { .. } if self.data_seen => {
                return Err(format!(
                    "{} is op {i}, after a Put or Delete; it is first within a record but for a RequireCkpt (S-STORE §12.3)",
                    op.name()
                ));
            }
            Op::CreateTree { .. } | Op::DropTree { .. } | Op::Ddl { .. } => {}
            Op::Delete { revert: true, .. } if !self.in_chunk => {
                return Err(format!(
                    "op {i} is a Delete with revert in a Commit; revert is Chunk only (S-STORE §12.3)"
                ));
            }
            Op::Put { .. } | Op::Delete { .. } => self.data_seen = true,
        }
        self.next = i.saturating_add(1);
        Ok(())
    }
}

/// The order rule and the `revert` rule over a whole list (§12.3): the
/// reason the first op that breaks them may not stand where it does.
pub(crate) fn check_list(ops: &[Op<'_>], in_chunk: bool) -> Result<(), String> {
    let mut order = OpOrder::new(in_chunk);
    ops.iter().try_for_each(|op| order.admit(op))
}

/// The encoded length of byte string `s`: its `u32` length and its bytes;
/// `Invalid` when it is longer than `u32::MAX` (card A-6a, Work 4).
fn string_len(s: &[u8]) -> Result<u64, StoreError> {
    u32::try_from(s.len())
        .map(|n| 4 + u64::from(n))
        .map_err(|_| {
            StoreError::Invalid(format!(
                "a WAL byte string of {} bytes is longer than u32::MAX (card A-6a)",
                s.len()
            ))
        })
}

/// The encoded length of an optional `before`: its presence byte and, if
/// present, its byte string.
fn before_len(before: Option<&[u8]>) -> Result<u64, StoreError> {
    before.map_or(Ok(0), string_len).map(|n| 1 + n)
}

/// The encoded length of `op`, its tag included.
fn op_len(op: &Op<'_>) -> Result<u64, StoreError> {
    Ok(match op {
        Op::CreateTree { .. } => 1 + 8 + 1,
        Op::DropTree { .. } => 1 + 8 + 8,
        Op::Ddl { bytes } => 1 + string_len(bytes)?,
        Op::Put {
            key, value, before, ..
        } => 1 + 8 + string_len(key)? + 1 + string_len(value)? + before_len(*before)?,
        Op::Delete { key, before, .. } => 1 + 8 + string_len(key)? + 1 + before_len(*before)?,
        Op::RequireCkpt { .. } => 1 + 8,
    })
}

/// The encoded length of a list of ops, checked: `Invalid` when a byte
/// string is longer than `u32::MAX` or the sum overflows.
pub(crate) fn list_len(ops: &[Op<'_>]) -> Result<u64, StoreError> {
    ops.iter().try_fold(0u64, |sum, op| {
        sum.checked_add(op_len(op)?).ok_or_else(|| {
            StoreError::Invalid("a WAL record's ops overflow u64 (card A-6a)".into())
        })
    })
}

/// Writes byte string `s`: its `u32` length, then its bytes.
fn write_string(out: &mut Writer, s: &[u8]) -> Result<(), StoreError> {
    let n = u32::try_from(s.len()).map_err(|_| {
        StoreError::Invalid(format!(
            "a WAL byte string of {} bytes is longer than u32::MAX (card A-6a)",
            s.len()
        ))
    })?;
    out.u32(n);
    out.bytes(s);
    Ok(())
}

/// Writes an optional `before`: its presence byte, then, if present, its
/// byte string.
fn write_before(out: &mut Writer, before: Option<&[u8]>) -> Result<(), StoreError> {
    match before {
        None => {
            out.u8(BEFORE_ABSENT);
            Ok(())
        }
        Some(b) => {
            out.u8(BEFORE_PRESENT);
            write_string(out, b)
        }
    }
}

/// Writes `ops` in the pinned encoding. The caller has checked the order
/// rule.
pub(crate) fn write_list(out: &mut Writer, ops: &[Op<'_>]) -> Result<(), StoreError> {
    for op in ops {
        match op {
            Op::CreateTree { tree_id, kind } => {
                out.u8(TAG_CREATE_TREE);
                out.u64(*tree_id);
                out.u8(*kind);
            }
            Op::DropTree { tree_id, w } => {
                out.u8(TAG_DROP_TREE);
                out.u64(*tree_id);
                out.u64(*w);
            }
            Op::Ddl { bytes } => {
                out.u8(TAG_DDL);
                write_string(out, bytes)?;
            }
            Op::Put {
                tree_id,
                key,
                key_changed,
                value,
                before,
            } => {
                out.u8(TAG_PUT);
                out.u64(*tree_id);
                write_string(out, key)?;
                out.u8(if *key_changed { PUT_KEY_CHANGED } else { 0 });
                write_string(out, value)?;
                write_before(out, *before)?;
            }
            Op::Delete {
                tree_id,
                key,
                moved,
                revert,
                before,
            } => {
                out.u8(TAG_DELETE);
                out.u64(*tree_id);
                write_string(out, key)?;
                let mut flags = 0;
                if *moved {
                    flags |= DELETE_MOVED;
                }
                if *revert {
                    flags |= DELETE_REVERT;
                }
                out.u8(flags);
                write_before(out, *before)?;
            }
            Op::RequireCkpt { ckpt_id } => {
                out.u8(TAG_REQUIRE_CKPT);
                out.u64(*ckpt_id);
            }
        }
    }
    Ok(())
}

/// Reads a byte string: a `u32` length, then that many bytes, all inside
/// the payload.
fn read_string<'a>(r: &mut Reader<'a>) -> Result<&'a [u8], StoreError> {
    let n = r.u32()?;
    // A `u32` length is widened, never narrowed: on a 16-bit target a
    // length past `usize::MAX` cannot fit any payload.
    let n = usize::try_from(n).map_err(|_| {
        StoreError::Corrupt(format!("a byte string of {n} bytes runs past the payload"))
    })?;
    r.bytes(n)
}

/// Reads an optional `before`: a presence byte of 0 or 1, then, if 1, a
/// byte string.
fn read_before<'a>(r: &mut Reader<'a>) -> Result<Option<&'a [u8]>, StoreError> {
    match r.u8()? {
        BEFORE_ABSENT => Ok(None),
        BEFORE_PRESENT => read_string(r).map(Some),
        p => Err(StoreError::Corrupt(format!(
            "presence byte {p} is neither 0 (absent) nor 1 (present) (card A-6a)"
        ))),
    }
}

/// Reads one op at the reader's position.
fn read_op<'a>(r: &mut Reader<'a>) -> Result<Op<'a>, StoreError> {
    let tag = r.u8()?;
    match tag {
        TAG_CREATE_TREE => {
            let tree_id = r.u64()?;
            let kind = r.u8()?;
            Ok(Op::CreateTree { tree_id, kind })
        }
        TAG_DROP_TREE => {
            let tree_id = r.u64()?;
            let w = r.u64()?;
            Ok(Op::DropTree { tree_id, w })
        }
        TAG_DDL => Ok(Op::Ddl {
            bytes: read_string(r)?,
        }),
        TAG_PUT => {
            let tree_id = r.u64()?;
            let key = read_string(r)?;
            let flags = r.u8()?;
            if flags & !PUT_KEY_CHANGED != 0 {
                return Err(StoreError::Corrupt(format!(
                    "a Put's flags {flags:#04x} set a bit other than bit 0 (card A-6a)"
                )));
            }
            let value = read_string(r)?;
            let before = read_before(r)?;
            Ok(Op::Put {
                tree_id,
                key,
                key_changed: flags & PUT_KEY_CHANGED != 0,
                value,
                before,
            })
        }
        TAG_DELETE => {
            let tree_id = r.u64()?;
            let key = read_string(r)?;
            let flags = r.u8()?;
            if flags & !(DELETE_MOVED | DELETE_REVERT) != 0 {
                return Err(StoreError::Corrupt(format!(
                    "a Delete's flags {flags:#04x} set a bit other than bits 0 and 1 (card A-6a)"
                )));
            }
            let before = read_before(r)?;
            Ok(Op::Delete {
                tree_id,
                key,
                moved: flags & DELETE_MOVED != 0,
                revert: flags & DELETE_REVERT != 0,
                before,
            })
        }
        TAG_REQUIRE_CKPT => Ok(Op::RequireCkpt { ckpt_id: r.u64()? }),
        other => Err(StoreError::Corrupt(format!(
            "op tag {other} is not an op (S-STORE §12.3; card A-6a)"
        ))),
    }
}

/// Reads ops until the payload ends, each wholly inside it and admitted by
/// the order rule; every failure is `Corrupt`, naming the op's index and
/// its offset in the payload.
pub(crate) fn read_list<'a>(r: &mut Reader<'a>, in_chunk: bool) -> Result<Vec<Op<'a>>, StoreError> {
    let mut order = OpOrder::new(in_chunk);
    let mut ops = Vec::new();
    while r.remaining() > 0 {
        let at = r.pos();
        let op = read_op(r).map_err(|e| match e {
            StoreError::Corrupt(why) => {
                StoreError::Corrupt(format!("op {} at payload offset {at}: {why}", ops.len()))
            }
            other => other,
        })?;
        order
            .admit(&op)
            .map_err(|why| StoreError::Corrupt(format!("op at payload offset {at}: {why}")))?;
        ops.push(op);
    }
    Ok(ops)
}
