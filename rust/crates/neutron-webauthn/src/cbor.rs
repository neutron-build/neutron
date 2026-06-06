//! Minimal CBOR decoder for WebAuthn COSE public keys.
//!
//! Only supports the subset needed to parse ES256 (P-256) public keys:
//! - Map with integer keys
//! - Integer values (positive and negative)
//! - Byte-string values
//!
//! COSE_Key (RFC 8152) for ES256:
//! ```text
//! {
//!   1: 2,       // kty = EC2
//!   3: -7,      // alg = ES256
//!  -1: 1,       // crv = P-256
//!  -2: <32 bytes x>,
//!  -3: <32 bytes y>,
//! }
//! ```

use crate::error::WebAuthnError;

/// A parsed COSE public key (EC2 / ES256 only).
#[derive(Debug, Clone)]
pub struct CoseKey {
    /// Key type (must be 2 = EC2).
    pub kty: i64,
    /// Algorithm (must be -7 = ES256).
    pub alg: i64,
    /// Curve (must be 1 = P-256).
    pub crv: i64,
    /// X coordinate (32 bytes).
    pub x: Vec<u8>,
    /// Y coordinate (32 bytes).
    pub y: Vec<u8>,
}

/// Parse a COSE_Key from CBOR bytes.
pub fn parse_cose_key(data: &[u8]) -> Result<CoseKey, WebAuthnError> {
    let mut pos = 0usize;

    // Expect a map
    let map_len = read_map_header(data, &mut pos)?;

    let mut kty: Option<i64> = None;
    let mut alg: Option<i64> = None;
    let mut crv: Option<i64> = None;
    let mut x:   Option<Vec<u8>> = None;
    let mut y:   Option<Vec<u8>> = None;

    for _ in 0..map_len {
        let key = read_int(data, &mut pos)?;
        match key {
            1  => kty = Some(read_int(data, &mut pos)?),
            3  => alg = Some(read_int(data, &mut pos)?),
            -1 => crv = Some(read_int(data, &mut pos)?),
            -2 => x   = Some(read_bytes(data, &mut pos)?),
            -3 => y   = Some(read_bytes(data, &mut pos)?),
            _  => { skip_value(data, &mut pos)?; }
        }
    }

    Ok(CoseKey {
        kty: kty.ok_or_else(|| WebAuthnError::MissingField("kty".into()))?,
        alg: alg.ok_or_else(|| WebAuthnError::MissingField("alg".into()))?,
        crv: crv.ok_or_else(|| WebAuthnError::MissingField("crv".into()))?,
        x:   x  .ok_or_else(|| WebAuthnError::MissingField("x".into()))?,
        y:   y  .ok_or_else(|| WebAuthnError::MissingField("y".into()))?,
    })
}

// ---------------------------------------------------------------------------
// CBOR primitives
// ---------------------------------------------------------------------------

fn peek(data: &[u8], pos: usize) -> Result<u8, WebAuthnError> {
    data.get(pos).copied().ok_or_else(|| WebAuthnError::Cbor("unexpected end of data".into()))
}

fn read_map_header(data: &[u8], pos: &mut usize) -> Result<usize, WebAuthnError> {
    let b = peek(data, *pos)?;
    let major = b >> 5;
    if major != 5 {
        return Err(WebAuthnError::Cbor(format!("expected map (major 5), got major {major}")));
    }
    let (len, advance) = decode_additional(data, *pos)?;
    *pos += advance;
    Ok(len as usize)
}

fn read_int(data: &[u8], pos: &mut usize) -> Result<i64, WebAuthnError> {
    let b = peek(data, *pos)?;
    let major = b >> 5;
    match major {
        0 => { // positive integer
            let (v, advance) = decode_additional(data, *pos)?;
            *pos += advance;
            Ok(v as i64)
        }
        1 => { // negative integer: -1 - v
            let (v, advance) = decode_additional(data, *pos)?;
            *pos += advance;
            Ok(-1 - v as i64)
        }
        _ => Err(WebAuthnError::Cbor(format!("expected integer (major 0 or 1), got major {major}")))
    }
}

fn read_bytes(data: &[u8], pos: &mut usize) -> Result<Vec<u8>, WebAuthnError> {
    let b = peek(data, *pos)?;
    let major = b >> 5;
    if major != 2 {
        return Err(WebAuthnError::Cbor(format!("expected bytes (major 2), got major {major}")));
    }
    let (len, advance) = decode_additional(data, *pos)?;
    *pos += advance;
    let len = len as usize;
    if *pos + len > data.len() {
        return Err(WebAuthnError::Cbor("byte string overruns buffer".into()));
    }
    let out = data[*pos..*pos + len].to_vec();
    *pos += len;
    Ok(out)
}

fn skip_value(data: &[u8], pos: &mut usize) -> Result<(), WebAuthnError> {
    let b = peek(data, *pos)?;
    let major = b >> 5;
    match major {
        0 | 1 => { read_int(data, pos)?; }
        2 | 3 => {
            let (len, advance) = decode_additional(data, *pos)?;
            *pos += advance + len as usize;
        }
        5 => {
            let (map_len, advance) = decode_additional(data, *pos)?;
            *pos += advance;
            for _ in 0..(map_len * 2) { skip_value(data, pos)?; }
        }
        4 => {
            let (arr_len, advance) = decode_additional(data, *pos)?;
            *pos += advance;
            for _ in 0..arr_len { skip_value(data, pos)?; }
        }
        _ => return Err(WebAuthnError::Cbor(format!("unsupported major type {major}"))),
    }
    Ok(())
}

/// Returns `(value, bytes_consumed)` including the initial byte.
fn decode_additional(data: &[u8], pos: usize) -> Result<(u64, usize), WebAuthnError> {
    let b = peek(data, pos)?;
    let info = b & 0x1f;
    match info {
        0..=23 => Ok((info as u64, 1)),
        24 => {
            let v = *data.get(pos + 1).ok_or_else(|| WebAuthnError::Cbor("truncated".into()))? as u64;
            Ok((v, 2))
        }
        25 => {
            let hi = *data.get(pos + 1).ok_or_else(|| WebAuthnError::Cbor("truncated".into()))? as u64;
            let lo = *data.get(pos + 2).ok_or_else(|| WebAuthnError::Cbor("truncated".into()))? as u64;
            Ok((hi << 8 | lo, 3))
        }
        _ => Err(WebAuthnError::Cbor(format!("unsupported additional info {info}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal CBOR map for a COSE ES256 key.
    /// Encodes: {1:2, 3:-7, -1:1, -2:<x_bytes>, -3:<y_bytes>}
    fn build_cose_key(x: &[u8; 32], y: &[u8; 32]) -> Vec<u8> {
        let mut v = vec![];
        // map(5)
        v.push(0xa5);
        // 1: 2
        v.push(0x01); v.push(0x02);
        // 3: -7  → negative int -7 = major 1, value 6
        v.push(0x03); v.push(0x26); // 0x26 = major 1, additional 6
        // -1: 1  → key=-1 = major 1, value 0 = 0x20; value 1
        v.push(0x20); v.push(0x01);
        // -2: x  → key=-2 = 0x21; bstr(32) = 0x58, 0x20
        v.push(0x21); v.push(0x58); v.push(32); v.extend_from_slice(x);
        // -3: y  → key=-3 = 0x22; bstr(32)
        v.push(0x22); v.push(0x58); v.push(32); v.extend_from_slice(y);
        v
    }

    #[test]
    fn parse_cose_key_ok() {
        let x = [0x01u8; 32];
        let y = [0x02u8; 32];
        let cbor = build_cose_key(&x, &y);
        let key = parse_cose_key(&cbor).unwrap();
        assert_eq!(key.kty, 2);
        assert_eq!(key.alg, -7);
        assert_eq!(key.crv, 1);
        assert_eq!(key.x, x);
        assert_eq!(key.y, y);
    }

    #[test]
    fn parse_empty_fails() {
        assert!(parse_cose_key(&[]).is_err());
    }

    #[test]
    fn parse_wrong_type_fails() {
        // Push an integer instead of a map
        assert!(parse_cose_key(&[0x01]).is_err());
    }

    #[test]
    fn decode_additional_small() {
        // 0x17 = major 0, additional 23 → value=23, advance=1
        let (v, adv) = decode_additional(&[0x17], 0).unwrap();
        assert_eq!(v, 23);
        assert_eq!(adv, 1);
    }

    #[test]
    fn decode_additional_one_byte() {
        // 0x18, 0x64 → major 0, 1-byte follows = 100
        let (v, adv) = decode_additional(&[0x18, 0x64], 0).unwrap();
        assert_eq!(v, 100);
        assert_eq!(adv, 2);
    }

    #[test]
    fn negative_int_encoding() {
        // -7 in CBOR is 0x26 (major 1, additional 6)
        let mut pos = 0;
        let v = read_int(&[0x26], &mut pos).unwrap();
        assert_eq!(v, -7);
    }

    #[test]
    fn positive_int_encoding() {
        let mut pos = 0;
        let v = read_int(&[0x02], &mut pos).unwrap();
        assert_eq!(v, 2);
    }
}
