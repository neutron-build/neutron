//! AWS Signature Version 4 verification for the S3 gateway.
//!
//! Supports both authentication forms S3 clients use:
//!   - `Authorization: AWS4-HMAC-SHA256 ...` header signing
//!   - presigned URLs (`X-Amz-Algorithm=AWS4-HMAC-SHA256` query signing)
//!
//! plus verification of `aws-chunked` streaming payload chunk signatures
//! (`STREAMING-AWS4-HMAC-SHA256-PAYLOAD`).
//!
//! The gateway accepts any region string — the signature is verified against
//! the scope the client signed with, so `aws s3 --region anything` works.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

pub const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";
pub const STREAMING_PAYLOAD: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";

/// Allowed clock skew for `x-amz-date` (matches AWS: 15 minutes).
const MAX_SKEW_SECS: i64 = 15 * 60;

pub fn sha256_hex(data: &[u8]) -> String {
    hex(&Sha256::digest(data))
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("hmac accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// URI-encode per AWS rules: unreserved chars pass through; everything else
/// is %XX (uppercase hex). `/` passes through only when `encode_slash` is
/// false (path encoding).
pub fn aws_uri_encode(s: &str, encode_slash: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char)
            }
            b'/' if !encode_slash => out.push('/'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Percent-decode a URI component (`%XX` sequences; `+` is NOT space here —
/// S3 canonicalization treats query values as already-decoded strings).
pub fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let h = (bytes[i + 1] as char).to_digit(16);
            let l = (bytes[i + 2] as char).to_digit(16);
            if let (Some(h), Some(l)) = (h, l) {
                out.push((h * 16 + l) as u8);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Parsed authentication data, from either the Authorization header or
/// presigned query parameters.
#[derive(Debug, Clone)]
pub struct AuthData {
    pub access_key: String,
    /// `YYYYMMDD/region/service/aws4_request`
    pub scope: String,
    pub signed_headers: Vec<String>,
    pub signature: String,
    /// `YYYYMMDDTHHMMSSZ` from `x-amz-date` (or the Date header fallback).
    pub amz_date: String,
    /// Presigned only: validity window in seconds.
    pub expires: Option<u64>,
    pub presigned: bool,
}

/// Parse an `Authorization: AWS4-HMAC-SHA256 Credential=..., SignedHeaders=...,
/// Signature=...` header.
pub fn parse_authorization_header(value: &str, amz_date: &str) -> Result<AuthData, String> {
    let rest = value
        .strip_prefix("AWS4-HMAC-SHA256")
        .ok_or("unsupported authorization scheme")?
        .trim();
    let mut credential = None;
    let mut signed_headers = None;
    let mut signature = None;
    for part in rest.split(',') {
        let part = part.trim();
        if let Some(v) = part.strip_prefix("Credential=") {
            credential = Some(v.to_string());
        } else if let Some(v) = part.strip_prefix("SignedHeaders=") {
            signed_headers = Some(v.to_string());
        } else if let Some(v) = part.strip_prefix("Signature=") {
            signature = Some(v.to_string());
        }
    }
    let credential = credential.ok_or("missing Credential")?;
    let (access_key, scope) = credential
        .split_once('/')
        .ok_or("malformed Credential scope")?;
    Ok(AuthData {
        access_key: access_key.to_string(),
        scope: scope.to_string(),
        signed_headers: signed_headers
            .ok_or("missing SignedHeaders")?
            .split(';')
            .map(|s| s.to_string())
            .collect(),
        signature: signature.ok_or("missing Signature")?,
        amz_date: amz_date.to_string(),
        expires: None,
        presigned: false,
    })
}

/// Parse presigned-URL auth from decoded query parameters.
pub fn parse_presigned_query(params: &[(String, String)]) -> Result<AuthData, String> {
    let get = |name: &str| {
        params
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
    };
    let algorithm = get("X-Amz-Algorithm").ok_or("missing X-Amz-Algorithm")?;
    if algorithm != "AWS4-HMAC-SHA256" {
        return Err("unsupported algorithm".into());
    }
    let credential = get("X-Amz-Credential").ok_or("missing X-Amz-Credential")?;
    let (access_key, scope) = credential
        .split_once('/')
        .ok_or("malformed X-Amz-Credential")?;
    let expires: u64 = get("X-Amz-Expires")
        .ok_or("missing X-Amz-Expires")?
        .parse()
        .map_err(|_| "bad X-Amz-Expires")?;
    Ok(AuthData {
        access_key: access_key.to_string(),
        scope: scope.to_string(),
        signed_headers: get("X-Amz-SignedHeaders")
            .ok_or("missing X-Amz-SignedHeaders")?
            .split(';')
            .map(|s| s.to_string())
            .collect(),
        signature: get("X-Amz-Signature").ok_or("missing X-Amz-Signature")?,
        amz_date: get("X-Amz-Date").ok_or("missing X-Amz-Date")?,
        expires: Some(expires),
        presigned: true,
    })
}

/// Derive the SigV4 signing key for a secret and credential scope.
fn signing_key(secret: &str, scope: &str) -> Result<Vec<u8>, String> {
    let mut parts = scope.split('/');
    let date = parts.next().ok_or("scope missing date")?;
    let region = parts.next().ok_or("scope missing region")?;
    let service = parts.next().ok_or("scope missing service")?;
    let terminator = parts.next().ok_or("scope missing terminator")?;
    if terminator != "aws4_request" {
        return Err("scope terminator must be aws4_request".into());
    }
    let k_date = hmac_sha256(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    Ok(hmac_sha256(&k_service, b"aws4_request"))
}

/// Everything needed to reconstruct the canonical request.
pub struct CanonicalRequestInput<'a> {
    pub method: &'a str,
    /// Raw (still percent-encoded) request path, e.g. `/bucket/some%20key`.
    pub raw_path: &'a str,
    /// Decoded query parameters (excluding `X-Amz-Signature` for presigned).
    pub query: &'a [(String, String)],
    /// Lowercased header name → value (as received).
    pub headers: &'a [(String, String)],
    pub payload_hash: &'a str,
}

/// Build the canonical request string.
fn canonical_request(input: &CanonicalRequestInput<'_>, signed_headers: &[String]) -> String {
    // Canonical URI: decode then re-encode each path segment per AWS rules.
    let decoded = percent_decode(input.raw_path);
    let canonical_uri = if decoded.is_empty() {
        "/".to_string()
    } else {
        aws_uri_encode(&decoded, false)
    };

    // Canonical query: sorted by encoded name, then encoded value.
    let mut encoded_params: Vec<(String, String)> = input
        .query
        .iter()
        .map(|(k, v)| (aws_uri_encode(k, true), aws_uri_encode(v, true)))
        .collect();
    encoded_params.sort();
    let canonical_query = encoded_params
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");

    // Canonical headers: the signed ones, lowercased, sorted, trimmed values.
    let mut canonical_headers = String::new();
    let mut names: Vec<String> = signed_headers.iter().map(|h| h.to_lowercase()).collect();
    names.sort();
    for name in &names {
        let value = input
            .headers
            .iter()
            .filter(|(k, _)| k == name)
            .map(|(_, v)| v.trim())
            .collect::<Vec<_>>()
            .join(",");
        canonical_headers.push_str(name);
        canonical_headers.push(':');
        canonical_headers.push_str(&value);
        canonical_headers.push('\n');
    }
    let signed_headers_str = names.join(";");

    format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        input.method,
        canonical_uri,
        canonical_query,
        canonical_headers,
        signed_headers_str,
        input.payload_hash
    )
}

/// Compute the SigV4 signature for a canonical request. Public so clients
/// (tests, SDK helpers, presigned-URL generation) can produce signatures
/// with the same code that verifies them.
pub fn compute_signature(
    secret: &str,
    auth: &AuthData,
    input: &CanonicalRequestInput<'_>,
) -> Result<String, String> {
    let creq = canonical_request(input, &auth.signed_headers);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        auth.amz_date,
        auth.scope,
        sha256_hex(creq.as_bytes())
    );
    let key = signing_key(secret, &auth.scope)?;
    Ok(hex(&hmac_sha256(&key, string_to_sign.as_bytes())))
}

/// Constant-time-ish comparison (both are short hex strings; XOR-fold to
/// avoid early-exit timing on the signature check).
fn eq_ct(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.bytes()
        .zip(b.bytes())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

/// Parse `YYYYMMDDTHHMMSSZ` into a unix timestamp.
pub fn parse_amz_date(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    if b.len() != 16 || b[8] != b'T' || b[15] != b'Z' {
        return None;
    }
    let num = |r: std::ops::Range<usize>| s.get(r)?.parse::<i64>().ok();
    let (year, month, day) = (num(0..4)?, num(4..6)?, num(6..8)?);
    let (hour, min, sec) = (num(9..11)?, num(11..13)?, num(13..15)?);
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    // Days since epoch (civil-from-days inverse, Howard Hinnant's algorithm).
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe - 719468;
    Some(days * 86400 + hour * 3600 + min * 60 + sec)
}

/// Verify a request signature. `now` is the current unix timestamp (injected
/// for testability). Returns the error message for a 403 on failure.
pub fn verify(
    secret: &str,
    auth: &AuthData,
    input: &CanonicalRequestInput<'_>,
    now: i64,
) -> Result<(), String> {
    // Scope date must match the x-amz-date day.
    let scope_date = auth.scope.split('/').next().unwrap_or("");
    if auth.amz_date.len() < 8 || scope_date != &auth.amz_date[..8] {
        return Err("credential scope date mismatch".into());
    }
    let ts = parse_amz_date(&auth.amz_date).ok_or("malformed x-amz-date")?;
    if auth.presigned {
        let expires = auth.expires.unwrap_or(0) as i64;
        if !(0..=604800).contains(&expires) {
            return Err("X-Amz-Expires out of range".into());
        }
        if now < ts - MAX_SKEW_SECS || now > ts + expires {
            return Err("presigned URL expired".into());
        }
    } else if (now - ts).abs() > MAX_SKEW_SECS {
        return Err("request time too skewed".into());
    }
    let expected = compute_signature(secret, auth, input)?;
    if eq_ct(&expected, &auth.signature) {
        Ok(())
    } else {
        Err("signature does not match".into())
    }
}

/// Verify and strip `aws-chunked` framing, returning the decoded payload.
///
/// Frame format per chunk:
/// `hex-size;chunk-signature=<sig>\r\n<data>\r\n`, terminated by a zero-size
/// chunk. Each chunk signature chains from the previous one (seed = the
/// request signature).
pub fn decode_aws_chunked(body: &[u8], secret: &str, auth: &AuthData) -> Result<Vec<u8>, String> {
    let key = signing_key(secret, &auth.scope)?;
    let mut prev_sig = auth.signature.clone();
    let mut out = Vec::new();
    let mut pos = 0usize;

    loop {
        let header_end = find_crlf(body, pos).ok_or("aws-chunked: missing chunk header")?;
        let header =
            std::str::from_utf8(&body[pos..header_end]).map_err(|_| "aws-chunked: bad header")?;
        let (size_hex, sig) = header
            .split_once(";chunk-signature=")
            .ok_or("aws-chunked: missing chunk-signature")?;
        let size =
            usize::from_str_radix(size_hex.trim(), 16).map_err(|_| "aws-chunked: bad size")?;
        let data_start = header_end + 2;
        let data_end = data_start + size;
        if body.len() < data_end + 2 {
            return Err("aws-chunked: truncated chunk".into());
        }
        let data = &body[data_start..data_end];

        // chunk string-to-sign
        let sts = format!(
            "AWS4-HMAC-SHA256-PAYLOAD\n{}\n{}\n{}\n{}\n{}",
            auth.amz_date,
            auth.scope,
            prev_sig,
            sha256_hex(b""),
            sha256_hex(data)
        );
        let expected = hex(&hmac_sha256(&key, sts.as_bytes()));
        if !eq_ct(&expected, sig.trim()) {
            return Err("aws-chunked: chunk signature mismatch".into());
        }
        prev_sig = expected;

        if size == 0 {
            return Ok(out);
        }
        out.extend_from_slice(data);
        pos = data_end + 2;
    }
}

fn find_crlf(data: &[u8], from: usize) -> Option<usize> {
    (from..data.len().saturating_sub(1)).find(|&i| data[i] == b'\r' && data[i + 1] == b'\n')
}

#[cfg(test)]
mod tests {
    use super::*;

    // AWS documentation test credentials (public example values).
    const ACCESS: &str = "AKIAIOSFODNN7EXAMPLE";
    const SECRET: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

    /// AWS docs: "Example: GET Object" header-signing test vector.
    #[test]
    fn aws_doc_vector_get_object() {
        let headers = vec![
            (
                "host".to_string(),
                "examplebucket.s3.amazonaws.com".to_string(),
            ),
            ("range".to_string(), "bytes=0-9".to_string()),
            (
                "x-amz-content-sha256".to_string(),
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".to_string(),
            ),
            ("x-amz-date".to_string(), "20130524T000000Z".to_string()),
        ];
        let auth = AuthData {
            access_key: ACCESS.to_string(),
            scope: "20130524/us-east-1/s3/aws4_request".to_string(),
            signed_headers: vec![
                "host".into(),
                "range".into(),
                "x-amz-content-sha256".into(),
                "x-amz-date".into(),
            ],
            signature: String::new(),
            amz_date: "20130524T000000Z".to_string(),
            expires: None,
            presigned: false,
        };
        let input = CanonicalRequestInput {
            method: "GET",
            raw_path: "/test.txt",
            query: &[],
            headers: &headers,
            payload_hash: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        };
        let sig = compute_signature(SECRET, &auth, &input).unwrap();
        assert_eq!(
            sig,
            "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
    }

    /// AWS docs: "Example: PUT Object" header-signing test vector.
    #[test]
    fn aws_doc_vector_put_object() {
        let payload_hash = sha256_hex(b"Welcome to Amazon S3.");
        let headers = vec![
            (
                "date".to_string(),
                "Fri, 24 May 2013 00:00:00 GMT".to_string(),
            ),
            (
                "host".to_string(),
                "examplebucket.s3.amazonaws.com".to_string(),
            ),
            ("x-amz-content-sha256".to_string(), payload_hash.clone()),
            ("x-amz-date".to_string(), "20130524T000000Z".to_string()),
            (
                "x-amz-storage-class".to_string(),
                "REDUCED_REDUNDANCY".to_string(),
            ),
        ];
        let auth = AuthData {
            access_key: ACCESS.to_string(),
            scope: "20130524/us-east-1/s3/aws4_request".to_string(),
            signed_headers: vec![
                "date".into(),
                "host".into(),
                "x-amz-content-sha256".into(),
                "x-amz-date".into(),
                "x-amz-storage-class".into(),
            ],
            signature: String::new(),
            amz_date: "20130524T000000Z".to_string(),
            expires: None,
            presigned: false,
        };
        let input = CanonicalRequestInput {
            method: "PUT",
            raw_path: "/test%24file.text",
            query: &[],
            headers: &headers,
            payload_hash: &payload_hash,
        };
        let sig = compute_signature(SECRET, &auth, &input).unwrap();
        assert_eq!(
            sig,
            "98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd"
        );
    }

    /// AWS docs: "Example: GET Bucket Lifecycle" test vector (query params).
    #[test]
    fn aws_doc_vector_get_lifecycle() {
        let empty_hash = sha256_hex(b"");
        let headers = vec![
            (
                "host".to_string(),
                "examplebucket.s3.amazonaws.com".to_string(),
            ),
            ("x-amz-content-sha256".to_string(), empty_hash.clone()),
            ("x-amz-date".to_string(), "20130524T000000Z".to_string()),
        ];
        let query = vec![("lifecycle".to_string(), String::new())];
        let auth = AuthData {
            access_key: ACCESS.to_string(),
            scope: "20130524/us-east-1/s3/aws4_request".to_string(),
            signed_headers: vec![
                "host".into(),
                "x-amz-content-sha256".into(),
                "x-amz-date".into(),
            ],
            signature: String::new(),
            amz_date: "20130524T000000Z".to_string(),
            expires: None,
            presigned: false,
        };
        let input = CanonicalRequestInput {
            method: "GET",
            raw_path: "/",
            query: &query,
            headers: &headers,
            payload_hash: &empty_hash,
        };
        let sig = compute_signature(SECRET, &auth, &input).unwrap();
        assert_eq!(
            sig,
            "fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543"
        );
    }

    /// AWS docs: presigned-URL test vector.
    #[test]
    fn aws_doc_vector_presigned() {
        let headers = vec![(
            "host".to_string(),
            "examplebucket.s3.amazonaws.com".to_string(),
        )];
        let query = vec![
            (
                "X-Amz-Algorithm".to_string(),
                "AWS4-HMAC-SHA256".to_string(),
            ),
            (
                "X-Amz-Credential".to_string(),
                format!("{ACCESS}/20130524/us-east-1/s3/aws4_request"),
            ),
            ("X-Amz-Date".to_string(), "20130524T000000Z".to_string()),
            ("X-Amz-Expires".to_string(), "86400".to_string()),
            ("X-Amz-SignedHeaders".to_string(), "host".to_string()),
        ];
        let auth = AuthData {
            access_key: ACCESS.to_string(),
            scope: "20130524/us-east-1/s3/aws4_request".to_string(),
            signed_headers: vec!["host".into()],
            signature: String::new(),
            amz_date: "20130524T000000Z".to_string(),
            expires: Some(86400),
            presigned: true,
        };
        let input = CanonicalRequestInput {
            method: "GET",
            raw_path: "/test.txt",
            query: &query,
            headers: &headers,
            payload_hash: UNSIGNED_PAYLOAD,
        };
        let sig = compute_signature(SECRET, &auth, &input).unwrap();
        assert_eq!(
            sig,
            "aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"
        );
    }

    /// AWS docs: aws-chunked (streaming) upload — seed signature vector.
    #[test]
    fn aws_doc_vector_streaming_seed() {
        let headers = vec![
            ("content-encoding".to_string(), "aws-chunked".to_string()),
            ("content-length".to_string(), "66824".to_string()),
            ("host".to_string(), "s3.amazonaws.com".to_string()),
            (
                "x-amz-content-sha256".to_string(),
                STREAMING_PAYLOAD.to_string(),
            ),
            ("x-amz-date".to_string(), "20130524T000000Z".to_string()),
            (
                "x-amz-decoded-content-length".to_string(),
                "66560".to_string(),
            ),
            (
                "x-amz-storage-class".to_string(),
                "REDUCED_REDUNDANCY".to_string(),
            ),
        ];
        let auth = AuthData {
            access_key: ACCESS.to_string(),
            scope: "20130524/us-east-1/s3/aws4_request".to_string(),
            signed_headers: vec![
                "content-encoding".into(),
                "content-length".into(),
                "host".into(),
                "x-amz-content-sha256".into(),
                "x-amz-date".into(),
                "x-amz-decoded-content-length".into(),
                "x-amz-storage-class".into(),
            ],
            signature: String::new(),
            amz_date: "20130524T000000Z".to_string(),
            expires: None,
            presigned: false,
        };
        let input = CanonicalRequestInput {
            method: "PUT",
            raw_path: "/examplebucket/chunkObject.txt",
            query: &[],
            headers: &headers,
            payload_hash: STREAMING_PAYLOAD,
        };
        let sig = compute_signature(SECRET, &auth, &input).unwrap();
        assert_eq!(
            sig,
            "4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9"
        );
    }

    /// AWS docs: aws-chunked chunk-signature chain (65536 'a' + 1024 'a' + 0).
    #[test]
    fn aws_doc_vector_streaming_chunks() {
        let auth = AuthData {
            access_key: ACCESS.to_string(),
            scope: "20130524/us-east-1/s3/aws4_request".to_string(),
            signed_headers: vec![],
            signature: "4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9"
                .to_string(),
            amz_date: "20130524T000000Z".to_string(),
            expires: None,
            presigned: false,
        };
        // Assemble the documented wire format.
        let chunk1 = vec![b'a'; 65536];
        let chunk2 = vec![b'a'; 1024];
        let mut body = Vec::new();
        body.extend_from_slice(
            b"10000;chunk-signature=ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648\r\n",
        );
        body.extend_from_slice(&chunk1);
        body.extend_from_slice(b"\r\n");
        body.extend_from_slice(
            b"400;chunk-signature=0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497\r\n",
        );
        body.extend_from_slice(&chunk2);
        body.extend_from_slice(b"\r\n");
        body.extend_from_slice(
            b"0;chunk-signature=b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9\r\n",
        );
        body.extend_from_slice(b"\r\n");

        let decoded = decode_aws_chunked(&body, SECRET, &auth).unwrap();
        assert_eq!(decoded.len(), 66560);
        assert!(decoded.iter().all(|&b| b == b'a'));

        // A tampered chunk must fail.
        let mut bad = body.clone();
        let tamper_at = 90; // inside chunk1 data
        bad[tamper_at] ^= 1;
        assert!(decode_aws_chunked(&bad, SECRET, &auth).is_err());
    }

    #[test]
    fn amz_date_parsing() {
        // 2013-05-24T00:00:00Z
        assert_eq!(parse_amz_date("20130524T000000Z"), Some(1369353600));
        // Epoch
        assert_eq!(parse_amz_date("19700101T000000Z"), Some(0));
        assert_eq!(parse_amz_date("20240229T120000Z"), Some(1709208000)); // leap day
        assert!(parse_amz_date("garbage").is_none());
        assert!(parse_amz_date("20131324T000000Z").is_none()); // month 13
    }

    #[test]
    fn verify_rejects_skew_and_expiry() {
        let headers = vec![("host".to_string(), "h".to_string())];
        let input = CanonicalRequestInput {
            method: "GET",
            raw_path: "/",
            query: &[],
            headers: &headers,
            payload_hash: UNSIGNED_PAYLOAD,
        };
        let mut auth = AuthData {
            access_key: "AK".to_string(),
            scope: "20130524/x/s3/aws4_request".to_string(),
            signed_headers: vec!["host".into()],
            signature: "00".to_string(),
            amz_date: "20130524T000000Z".to_string(),
            expires: None,
            presigned: false,
        };
        let ts = parse_amz_date("20130524T000000Z").unwrap();
        // Too skewed.
        assert!(verify("s", &auth, &input, ts + 3600).is_err());
        // In window but bad signature.
        let err = verify("s", &auth, &input, ts + 60).unwrap_err();
        assert!(err.contains("signature"), "{err}");
        // Presigned past expiry.
        auth.presigned = true;
        auth.expires = Some(100);
        assert!(
            verify("s", &auth, &input, ts + 101)
                .unwrap_err()
                .contains("expired")
        );
    }

    #[test]
    fn uri_encode_rules() {
        assert_eq!(aws_uri_encode("a b/c~d", false), "a%20b/c~d");
        assert_eq!(aws_uri_encode("a b/c~d", true), "a%20b%2Fc~d");
        assert_eq!(percent_decode("a%20b%2Fc"), "a b/c");
        assert_eq!(percent_decode("no-escapes"), "no-escapes");
        assert_eq!(percent_decode("bad%zz"), "bad%zz");
    }
}
