use super::*;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeSet, HashSet};

fn json(text: &str) -> Value {
    Value::Jsonb(serde_json::from_str(text).unwrap())
}

#[test]
fn x11_jsonb_numeric_equality_and_hash_are_exact_and_recursive() {
    for (left, right) in [
        ("1.0", "1.00"),
        ("1e2", "100.00"),
        ("-0.000e999999999999999999999999999999", "0"),
        (
            "1e999999999999999999999999999999",
            "10e999999999999999999999999999998",
        ),
        (
            "0.1e-999999999999999999999999999998",
            "1e-999999999999999999999999999999",
        ),
        (
            "{\"x\":[1.0,{\"n\":1234567890123456789012345678901234567890}]}",
            "{\"x\":[1.00,{\"n\":1234567890123456789012345678901234567890.0}]}",
        ),
    ] {
        let (a, b) = (json(left), json(right));
        assert_eq!(a, b, "{left} and {right}");
        let hash = |v: &Value| {
            let mut h = DefaultHasher::new();
            v.hash(&mut h);
            h.finish()
        };
        assert_eq!(hash(&a), hash(&b));
        assert_eq!(HashSet::from([a.clone(), b.clone()]).len(), 1);
        assert_eq!(BTreeSet::from([a, b]).len(), 1);
    }
    assert_ne!(
        json("1234567890123456789012345678901234567890"),
        json("1234567890123456789012345678901234567891")
    );
    assert_ne!(json("[1,2]"), json("[2,1]"));
    assert_ne!(json("1"), json("\"1\""));
}

#[test]
fn x11_jsonb_scientific_text_preserves_every_digit_and_scale() {
    for (input, expected) in [
        (
            "[1.123456789012345678901234567890123e0]",
            "[1.123456789012345678901234567890123]",
        ),
        ("[0.0012300e+2]", "[0.12300]"),
        ("[-0.00e0]", "[0.00]"),
        ("[-0, -0.00, -0e0, -0.00e0]", "[0, 0.00, 0, 0.00]"),
        ("[0.0012300e+8]", "[123000]"),
        ("[-0e3]", "[0]"),
        (
            "[1e999999999999999999999999999999]",
            "[1e999999999999999999999999999999]",
        ),
    ] {
        assert_eq!(json(input).to_string(), expected);
    }
    assert_eq!(json("1.00").to_string(), "1.00");
}
