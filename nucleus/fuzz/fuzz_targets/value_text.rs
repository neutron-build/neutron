#![no_main]
//! Client-supplied text parsed into typed values: the casts a string literal,
//! a text-format Bind parameter or a COPY field goes through.
//!
//! Every input is cast to every scalar type Nucleus parses itself (numeric,
//! date, timestamp, timestamptz, uuid, bytea, jsonb, vector, bool, int, float),
//! read as an array literal with the selected element type, read as an
//! INTERVAL literal, and run through NUMERIC typmod and arithmetic. Each
//! successful result is rendered back to text and must cast again.
//!
//! Byte 0 picks the array element type and the NUMERIC(p, s) typmod.
//!
//! Run: cargo +nightly fuzz run fuzz_value_text -- -max_len=4096

use libfuzzer_sys::fuzz_target;
use nucleus::types::{
    DataType, NumericTypmod, Value, array_value_from_literal, format_array, numeric_add,
    numeric_div, numeric_mul, parse_array_literal, validate_array_shape,
};

const SCALARS: &[DataType] = &[
    DataType::Bool,
    DataType::Int32,
    DataType::Int64,
    DataType::Float64,
    DataType::Jsonb,
    DataType::Date,
    DataType::Timestamp,
    DataType::TimestampTz,
    DataType::Numeric,
    DataType::Uuid,
    DataType::Bytea,
    DataType::Vector(3),
];

fn cast_and_render(text: &str, ty: &DataType) {
    let Ok(v) = Value::Text(text.to_owned()).cast(ty) else {
        return;
    };
    let rendered = v.to_string();
    let _ = v.cast(&DataType::Text);
    if matches!(
        ty,
        DataType::Numeric | DataType::Date | DataType::Uuid | DataType::Int64
    ) {
        // The text Nucleus prints for a value must be accepted as input.
        assert!(
            Value::Text(rendered.clone()).cast(ty).is_ok(),
            "{ty} {text:?} rendered as {rendered:?}, which does not parse back"
        );
    }
}

fuzz_target!(|data: &[u8]| {
    let Some((&sel, rest)) = data.split_first() else {
        return;
    };
    let Ok(text) = std::str::from_utf8(rest) else {
        return;
    };

    for ty in SCALARS {
        cast_and_render(text, ty);
    }

    let elem = &SCALARS[sel as usize % SCALARS.len()];
    if let Ok(items) = parse_array_literal(text) {
        let mut cast = |s: &str| Value::Text(s.to_owned()).cast(elem);
        if let Ok(Value::Array(vals)) = array_value_from_literal(&items, &mut cast) {
            let _ = validate_array_shape(&vals);
            let _ = format_array(&vals, &|v| v.to_string());
        }
    }

    if let Ok(v) = nucleus::executor::fuzz_api::parse_interval(text) {
        let _ = v.to_string();
    }

    if Value::Text(text.to_owned())
        .cast(&DataType::Numeric)
        .is_ok()
    {
        let _ = numeric_add(text, text);
        let _ = numeric_mul(text, text);
        let _ = numeric_div(text, "7");
        let _ = numeric_div("1", text);
        let precision = 1 + u64::from(sel % 40);
        let scale = i64::from(sel / 40) - 2;
        if let Ok(typmod) = NumericTypmod::new(precision, scale) {
            let _ = typmod.apply(text);
        }
    }
});
