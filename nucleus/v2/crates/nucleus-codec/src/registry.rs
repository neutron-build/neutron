//! The single type-kernel registry (C-Q3): one `Copy` row of function
//! pointers per `KeyType`, indexed by `TypeKind`.
//!
//! Totality is a compile-time property, not a lookup result:
//! - [`kind_of`] is an exhaustive `match` on `KeyType`, so adding a key type
//!   without a `TypeKind` does not compile;
//! - [`TYPE_OPS`] is an array of length [`TypeKind::COUNT`], so a missing row
//!   does not compile, and a const assertion checks that row `i` belongs to
//!   kind `i`;
//! - every row has all three slots (`compare`, `eq`, `hash`) because the row
//!   is built by one macro from one kind.
//!
//! Each slot is its own monomorphisation of a kernel that first checks the
//! declared type is the kind the row was built for, so a slot wired to
//! another type's kernel fails loudly (`TypeMismatch`) instead of silently
//! comparing the wrong thing. Order, equality and hash are never computed
//! here: every slot bottoms out in [`crate::kernels`], which defers to the
//! codec (C-Q3s P-ORDER, P-EQ, P-HASH).

use std::cmp::Ordering;

use crate::{kernels, CodecError, KeyType, Result, Value};

/// Operator OID space for the registry. Only the btree / equality / hash
/// slots of this card are populated; later cards add operators in this same
/// numbering without changing the key type.
pub type OpOid = u32;
/// btree support function 1: three-way comparison (`compare`).
pub const OP_BTREE_CMP: OpOid = 1;
/// Equality operator (`eq`), the grouping / DISTINCT / join-key rule.
pub const OP_EQ: OpOid = 2;
/// Hash support function (`hash`).
pub const OP_HASH: OpOid = 3;
/// Every operator slot populated for every type.
pub const POPULATED_OPS: [OpOid; 3] = [OP_BTREE_CMP, OP_EQ, OP_HASH];

/// `KeyType` without its payload (collation, array element type): the
/// registry index. Discriminants are the table indices.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum TypeKind {
    Bool = 0,
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    Numeric,
    Text,
    Bytea,
    Date,
    Time,
    Timestamp,
    TimestampTz,
    Interval,
    Uuid,
    Jsonb,
    Array,
}

impl TypeKind {
    pub const COUNT: usize = 17;
    /// All kinds in table order.
    pub const ALL: [TypeKind; TypeKind::COUNT] = [
        TypeKind::Bool,
        TypeKind::Int2,
        TypeKind::Int4,
        TypeKind::Int8,
        TypeKind::Float4,
        TypeKind::Float8,
        TypeKind::Numeric,
        TypeKind::Text,
        TypeKind::Bytea,
        TypeKind::Date,
        TypeKind::Time,
        TypeKind::Timestamp,
        TypeKind::TimestampTz,
        TypeKind::Interval,
        TypeKind::Uuid,
        TypeKind::Jsonb,
        TypeKind::Array,
    ];

    /// Index into [`TYPE_OPS`].
    pub const fn index(self) -> usize {
        self as usize
    }
}

/// The kind of a declared key type. Exhaustive on purpose.
pub const fn kind_of(ty: &KeyType) -> TypeKind {
    match ty {
        KeyType::Bool => TypeKind::Bool,
        KeyType::Int2 => TypeKind::Int2,
        KeyType::Int4 => TypeKind::Int4,
        KeyType::Int8 => TypeKind::Int8,
        KeyType::Float4 => TypeKind::Float4,
        KeyType::Float8 => TypeKind::Float8,
        KeyType::Numeric => TypeKind::Numeric,
        KeyType::Text(_) => TypeKind::Text,
        KeyType::Bytea => TypeKind::Bytea,
        KeyType::Date => TypeKind::Date,
        KeyType::Time => TypeKind::Time,
        KeyType::Timestamp => TypeKind::Timestamp,
        KeyType::TimestampTz => TypeKind::TimestampTz,
        KeyType::Interval => TypeKind::Interval,
        KeyType::Uuid => TypeKind::Uuid,
        KeyType::Jsonb => TypeKind::Jsonb,
        KeyType::Array(_) => TypeKind::Array,
    }
}

/// `compare` slot: btree three-way comparison of two non-NULL values.
pub type CompareFn = fn(&KeyType, &Value, &Value) -> Result<Ordering>;
/// `eq` slot: grouping equality of two non-NULL values.
pub type EqFn = fn(&KeyType, &Value, &Value) -> Result<bool>;
/// `hash` slot: the engine hash of one non-NULL value.
pub type HashFn = fn(&KeyType, &Value) -> Result<u64>;

/// One registry row. `Copy`, no trait objects.
#[derive(Clone, Copy)]
pub struct TypeOps {
    pub kind: TypeKind,
    pub compare: CompareFn,
    pub eq: EqFn,
    pub hash: HashFn,
}

impl std::fmt::Debug for TypeOps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypeOps")
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

/// Fails unless `ty` is the kind slot `K` was built for.
fn expect_kind<const K: usize>(ty: &KeyType) -> Result<()> {
    if kind_of(ty).index() == K {
        Ok(())
    } else {
        Err(CodecError::TypeMismatch)
    }
}

fn compare_slot<const K: usize>(ty: &KeyType, a: &Value, b: &Value) -> Result<Ordering> {
    expect_kind::<K>(ty)?;
    kernels::encoded_compare(ty, a, b)
}

fn eq_slot<const K: usize>(ty: &KeyType, a: &Value, b: &Value) -> Result<bool> {
    expect_kind::<K>(ty)?;
    kernels::encoded_eq(ty, a, b)
}

fn hash_slot<const K: usize>(ty: &KeyType, v: &Value) -> Result<u64> {
    expect_kind::<K>(ty)?;
    kernels::encoded_hash(ty, v)
}

/// One row, all three slots from one kind.
macro_rules! row {
    ($kind:ident) => {
        TypeOps {
            kind: TypeKind::$kind,
            compare: compare_slot::<{ TypeKind::$kind as usize }>,
            eq: eq_slot::<{ TypeKind::$kind as usize }>,
            hash: hash_slot::<{ TypeKind::$kind as usize }>,
        }
    };
}

/// The registry, one row per [`TypeKind`], in `TypeKind` order.
pub static TYPE_OPS: [TypeOps; TypeKind::COUNT] = [
    row!(Bool),
    row!(Int2),
    row!(Int4),
    row!(Int8),
    row!(Float4),
    row!(Float8),
    row!(Numeric),
    row!(Text),
    row!(Bytea),
    row!(Date),
    row!(Time),
    row!(Timestamp),
    row!(TimestampTz),
    row!(Interval),
    row!(Uuid),
    row!(Jsonb),
    row!(Array),
];

// Row `i` must be the row of kind `i` (and `ALL` must list kinds in order).
const _: () = {
    let mut i = 0;
    while i < TypeKind::COUNT {
        assert!(TypeKind::ALL[i].index() == i);
        assert!(TYPE_OPS[i].kind.index() == i);
        i += 1;
    }
};

/// The registry row for a declared type. Total: every `KeyType` has a row.
pub fn ops(ty: &KeyType) -> TypeOps {
    row_of(kind_of(ty))
}

/// The registry row of a kind.
pub fn row_of(kind: TypeKind) -> TypeOps {
    // In range by construction (`kind as usize < COUNT`, checked above).
    TYPE_OPS[kind.index()]
}
