//! Exact keys for JSONB equality and hashing, independent of display scale.
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

#[derive(Eq, PartialEq, Ord, PartialOrd, Hash)]
enum Key {
    Null,
    Bool(bool),
    Number(bool, String, String),
    String(String),
    Array(Vec<Key>),
    Object(Vec<(String, Key)>),
}

fn key(value: &serde_json::Value) -> Key {
    match value {
        serde_json::Value::Null => Key::Null,
        serde_json::Value::Bool(value) => Key::Bool(*value),
        serde_json::Value::Number(value) => {
            let (negative, digits, exponent) = number_key(&value.to_string());
            Key::Number(negative, digits, exponent)
        }
        serde_json::Value::String(value) => Key::String(value.clone()),
        serde_json::Value::Array(values) => Key::Array(values.iter().map(key).collect()),
        serde_json::Value::Object(values) => {
            let mut entries: Vec<_> = values
                .iter()
                .map(|(name, value)| (name.clone(), key(value)))
                .collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            Key::Object(entries)
        }
    }
}

/// Stable internal ordering of exact equality classes. This is not a claim of
/// PostgreSQL's JSONB ordering across unequal values.
pub(crate) fn compare(left: &serde_json::Value, right: &serde_json::Value) -> Ordering {
    key(left).cmp(&key(right))
}

pub(super) fn hash<H: Hasher>(value: &serde_json::Value, state: &mut H) {
    key(value).hash(state);
}

// A JSON number is sign * coefficient * 10^exponent. Keep the exponent as an
// arbitrary-length signed decimal integer: even huge exponents need no power
// expansion, floating point, bounded Decimal, or integer overflow.
fn number_key(text: &str) -> (bool, String, String) {
    let negative = text.starts_with('-');
    let unsigned = text.strip_prefix('-').unwrap_or(text);
    let (mantissa, exponent) = unsigned.split_once(['e', 'E']).unwrap_or((unsigned, "0"));
    let fraction = mantissa
        .split_once('.')
        .map_or(0, |(_, fractional)| fractional.len());
    let digits: String = mantissa.chars().filter(|ch| *ch != '.').collect();
    let digits = digits.trim_start_matches('0');
    if digits.is_empty() {
        return (false, "0".into(), "0".into());
    }
    let coefficient = digits.trim_end_matches('0');
    let shift = (digits.len() - coefficient.len()) as i128 - fraction as i128;
    (
        negative,
        coefficient.to_owned(),
        add_signed_decimal(exponent, &shift.to_string()),
    )
}

fn signed_digits(text: &str) -> (bool, &str) {
    let negative = text.starts_with('-');
    let digits = text.trim_start_matches(['+', '-']).trim_start_matches('0');
    if digits.is_empty() {
        (false, "0")
    } else {
        (negative, digits)
    }
}

fn add_signed_decimal(left: &str, right: &str) -> String {
    let (left_negative, left) = signed_digits(left);
    let (right_negative, right) = signed_digits(right);
    let magnitude_order = left.len().cmp(&right.len()).then_with(|| left.cmp(right));
    let (negative, mut digits) = if left_negative == right_negative {
        let mut sum = Vec::new();
        let mut carry = 0u8;
        let mut a = left.bytes().rev();
        let mut b = right.bytes().rev();
        loop {
            let (x, y) = (a.next(), b.next());
            if x.is_none() && y.is_none() {
                break;
            }
            let total = x.map_or(0, |n| n - b'0') + y.map_or(0, |n| n - b'0') + carry;
            sum.push(b'0' + total % 10);
            carry = total / 10;
        }
        if carry != 0 {
            sum.push(b'0' + carry);
        }
        (left_negative, sum)
    } else {
        let (larger, smaller, negative) = if magnitude_order.is_lt() {
            (right, left, right_negative)
        } else {
            (left, right, left_negative)
        };
        let mut difference = Vec::new();
        let mut b = smaller.bytes().rev();
        let mut borrow = 0i16;
        for a in larger.bytes().rev() {
            let mut digit =
                i16::from(a - b'0') - i16::from(b.next().map_or(0, |n| n - b'0')) - borrow;
            borrow = if digit < 0 {
                digit += 10;
                1
            } else {
                0
            };
            difference.push(b'0' + digit as u8);
        }
        while difference.len() > 1 && difference.last() == Some(&b'0') {
            difference.pop();
        }
        (negative, difference)
    };
    digits.reverse();
    let mut result = String::with_capacity(digits.len() + 1);
    if negative && digits != [b'0'] {
        result.push('-');
    }
    // Every byte above is an ASCII digit.
    result.extend(digits.into_iter().map(char::from));
    result
}

/// Expand scientific notation lexically, retaining all mantissa digits and
/// scale. Keep exact scientific notation when expansion would be too large.
pub(super) fn expanded_text(text: &str) -> Option<String> {
    const MAX_RENDER_BYTES: i128 = 16_384;
    let (mantissa, exponent) = text.split_once(['e', 'E'])?;
    let exponent = exponent.parse::<i128>().ok()?;
    let negative = mantissa.starts_with('-');
    let mantissa = mantissa.strip_prefix('-').unwrap_or(mantissa);
    let integer_digits = mantissa.find('.').unwrap_or(mantissa.len());
    let digits: String = mantissa.chars().filter(|ch| *ch != '.').collect();
    let point = (integer_digits as i128).checked_add(exponent)?;
    let output_len = if point <= 0 {
        2i128
            .checked_sub(point)?
            .checked_add(digits.len() as i128)?
    } else {
        point.max(digits.len() as i128).checked_add(1)?
    };
    let output_len = output_len.checked_add(i128::from(negative))?;
    if output_len > MAX_RENDER_BYTES {
        return None;
    }
    let mut out = String::with_capacity(output_len as usize);
    if negative && digits.bytes().any(|ch| ch != b'0') {
        out.push('-');
    }
    if point <= 0 {
        out.push_str("0.");
        out.extend(std::iter::repeat_n('0', (-point) as usize));
        out.push_str(&digits);
    } else if point >= digits.len() as i128 {
        let significant = digits.trim_start_matches('0');
        if significant.is_empty() {
            out.push('0');
        } else {
            out.push_str(significant);
            out.extend(std::iter::repeat_n('0', point as usize - digits.len()));
        }
    } else {
        let point = point as usize;
        let integer = digits[..point].trim_start_matches('0');
        out.push_str(if integer.is_empty() { "0" } else { integer });
        out.push('.');
        out.push_str(&digits[point..]);
    }
    Some(out)
}
