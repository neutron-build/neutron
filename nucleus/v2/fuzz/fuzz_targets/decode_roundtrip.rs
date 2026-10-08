#![no_main]
//! Card C-K2 work item 4: decoding arbitrary bytes must never panic, and
//! any successful decode must re-encode to exactly the bytes it consumed
//! (`decode_key_prefix` reports the consumed length; a commit-timestamp
//! suffix may follow, C-T0 §2.2).

use libfuzzer_sys::fuzz_target;
use nucleus_codec::{
    decode_key_prefix, encode_key, Collation, Direction, KeyColumn, KeyType, Nulls,
};
use std::sync::OnceLock;

static LAYOUTS: OnceLock<Vec<Vec<KeyColumn>>> = OnceLock::new();

/// Column lists covering every key type once, plus DESC, explicit NULLS
/// placement, arrays of two element types and a composite key.
fn layouts() -> &'static [Vec<KeyColumn>] {
    LAYOUTS.get_or_init(|| {
        let scalars = [
            KeyType::Bool,
            KeyType::Int2,
            KeyType::Int4,
            KeyType::Int8,
            KeyType::Float4,
            KeyType::Float8,
            KeyType::Numeric,
            KeyType::Text(Collation::C),
            KeyType::Bytea,
            KeyType::Date,
            KeyType::Time,
            KeyType::Timestamp,
            KeyType::TimestampTz,
            KeyType::Interval,
            KeyType::Uuid,
            KeyType::Jsonb,
        ];
        let mut out: Vec<Vec<KeyColumn>> = scalars
            .iter()
            .cloned()
            .map(|ty| vec![KeyColumn::asc(ty)])
            .collect();
        out.push(vec![KeyColumn::asc(KeyType::Array(Box::new(KeyType::Int4)))]);
        out.push(vec![KeyColumn::asc(KeyType::Array(Box::new(
            KeyType::Text(Collation::C),
        )))]);
        out.push(vec![KeyColumn::desc(KeyType::Float8)]);
        out.push(vec![KeyColumn::desc(KeyType::Numeric)]);
        out.push(vec![
            KeyColumn::asc(KeyType::Bytea).with_nulls(Nulls::First),
        ]);
        out.push(vec![
            KeyColumn::desc(KeyType::Time).with_nulls(Nulls::Last),
        ]);
        out.push(vec![
            KeyColumn::asc(KeyType::Int4),
            KeyColumn {
                dir: Direction::Desc,
                nulls: Nulls::First,
                ty: KeyType::Text(Collation::C),
            },
            KeyColumn::asc(KeyType::Float8),
        ]);
        out
    })
}

fuzz_target!(|data: &[u8]| {
    for cols in layouts() {
        if let Ok((vals, used)) = decode_key_prefix(cols, data) {
            let mut re = Vec::new();
            encode_key(cols, &vals, &mut re).unwrap_or_else(|e| panic!("re-encode: {e}"));
            assert_eq!(&data[..used], re.as_slice(), "decode round trip");
        }
    }
});
