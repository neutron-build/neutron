//! AWS Signature Version 4 (SigV4) signing.
//!
//! Implements canonical request, string-to-sign, derived signing key, and
//! both Authorization header signing and presigned URL query-param signing.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

// ---------------------------------------------------------------------------
// Low-level helpers
// ---------------------------------------------------------------------------

pub fn sha256_hex(data: &[u8]) -> String {
    hex_encode(&Sha256::digest(data))
}

pub fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key size");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

pub fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Derive the SigV4 signing key.
pub fn derive_signing_key(secret: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
    let k_secret  = format!("AWS4{secret}");
    let k_date    = hmac_sha256(k_secret.as_bytes(), date.as_bytes());
    let k_region  = hmac_sha256(&k_date,    region.as_bytes());
    let k_service = hmac_sha256(&k_region,  service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

/// Percent-encode a string for use in an AWS canonical query string or URL.
/// Encodes everything except unreserved characters: A-Z a-z 0-9 - _ . ~
pub fn uri_encode(s: &str, encode_slash: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' |
            b'-' | b'_' | b'.' | b'~' => out.push(b as char),
            b'/' if !encode_slash => out.push('/'),
            other => {
                out.push('%');
                out.push_str(&format!("{other:02X}"));
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Datetime helpers (no external dep — compute from epoch seconds)
// ---------------------------------------------------------------------------

/// Returns `(datetime, date)` as `("20240101T120000Z", "20240101")`.
pub fn utc_now() -> (String, String) {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    epoch_to_datetime(secs)
}

/// Convert a Unix timestamp (seconds) to `(datetime, date)`.
pub fn epoch_to_datetime(secs: u64) -> (String, String) {
    let sec  = (secs % 60) as u8;
    let min  = ((secs / 60) % 60) as u8;
    let hour = ((secs / 3600) % 24) as u8;
    let days = secs / 86400;
    let (year, month, day) = days_to_ymd(days);
    let dt   = format!("{year:04}{month:02}{day:02}T{hour:02}{min:02}{sec:02}Z");
    let date = dt[..8].to_string();
    (dt, date)
}

/// Convert days since 1970-01-01 to (year, month, day).
fn days_to_ymd(mut d: u64) -> (u32, u8, u8) {
    // 400-year cycle
    let n400 = d / 146097;
    d %= 146097;
    let n100 = (d / 36524).min(3);
    d -= n100 * 36524;
    let n4   = d / 1461;
    d %= 1461;
    let n1   = (d / 365).min(3);
    d -= n1 * 365;

    let year = (n400 * 400 + n100 * 100 + n4 * 4 + n1 + 1970) as u32;
    let leap  = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
    let days_in_month: [u8; 12] = [31, if leap { 29 } else { 28 }, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

    let mut month = 0u8;
    let mut day   = d as u8 + 1;
    for (i, &dim) in days_in_month.iter().enumerate() {
        if day <= dim {
            month = i as u8 + 1;
            break;
        }
        day -= dim;
    }
    (year, month, day)
}

// ---------------------------------------------------------------------------
// Authorization-header signing
// ---------------------------------------------------------------------------

/// Parameters for signing a request via the Authorization header.
pub struct AuthParams<'a> {
    pub method:       &'a str,
    pub host:         &'a str,
    /// URL-encoded path (e.g. `/bucket/path/to/key`).
    pub path:         &'a str,
    /// Pre-sorted, already-encoded canonical query string (empty if none).
    pub query:        &'a str,
    /// Additional headers to sign, sorted by name (lowercase).
    /// `host`, `x-amz-content-sha256`, and `x-amz-date` are always added.
    pub extra_headers: &'a [(&'a str, &'a str)],
    pub payload_hash: &'a str,
    pub datetime:     &'a str,
    pub date:         &'a str,
    pub region:       &'a str,
    pub access_key:   &'a str,
    pub secret_key:   &'a str,
}

/// Returns the `Authorization` header value and the sorted signed-header list
/// (needed for building the actual request headers).
pub fn authorization_header(p: &AuthParams<'_>) -> String {
    // Fixed headers that are always signed
    let mut headers: Vec<(&str, String)> = vec![
        ("host",                 p.host.to_string()),
        ("x-amz-content-sha256", p.payload_hash.to_string()),
        ("x-amz-date",           p.datetime.to_string()),
    ];
    for (k, v) in p.extra_headers {
        headers.push((k, v.to_string()));
    }
    headers.sort_by_key(|(k, _)| *k);

    let canonical_headers: String = headers.iter()
        .map(|(k, v)| format!("{}:{}\n", k, v.trim()))
        .collect();

    let signed_headers: String = headers.iter()
        .map(|(k, _)| *k)
        .collect::<Vec<_>>()
        .join(";");

    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        p.method, p.path, p.query, canonical_headers, signed_headers, p.payload_hash
    );

    let credential_scope = format!("{}/{}/s3/aws4_request", p.date, p.region);
    let string_to_sign   = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        p.datetime, credential_scope, sha256_hex(canonical_request.as_bytes())
    );

    let signing_key = derive_signing_key(p.secret_key, p.date, p.region, "s3");
    let signature   = hex_encode(&hmac_sha256(&signing_key, string_to_sign.as_bytes()));

    format!(
        "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
        p.access_key, credential_scope, signed_headers, signature
    )
}

// ---------------------------------------------------------------------------
// Presigned URL signing
// ---------------------------------------------------------------------------

/// Build a presigned URL (signature in query parameters, no Authorization header).
pub fn presigned_url(
    method:     &str,
    scheme:     &str,
    host:       &str,
    path:       &str,
    access_key: &str,
    secret_key: &str,
    region:     &str,
    datetime:   &str,
    date:       &str,
    expires:    u64,
    extra_query: &str,  // additional query params already canonical-encoded, or ""
) -> String {
    let credential_scope = format!("{date}/{region}/s3/aws4_request");
    let credential = format!("{access_key}/{credential_scope}");

    // Build canonical query string (must be sorted)
    let mut qparams: Vec<(String, String)> = vec![
        ("X-Amz-Algorithm".to_string(),  "AWS4-HMAC-SHA256".to_string()),
        ("X-Amz-Credential".to_string(), uri_encode(&credential, true)),
        ("X-Amz-Date".to_string(),       datetime.to_string()),
        ("X-Amz-Expires".to_string(),    expires.to_string()),
        ("X-Amz-SignedHeaders".to_string(), "host".to_string()),
    ];
    if !extra_query.is_empty() {
        for part in extra_query.split('&') {
            if let Some((k, v)) = part.split_once('=') {
                qparams.push((k.to_string(), v.to_string()));
            }
        }
    }
    qparams.sort_by(|a, b| a.0.cmp(&b.0));

    let canonical_query: String = qparams.iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");

    let canonical_headers = format!("host:{host}\n");
    let signed_headers    = "host";
    let payload_hash      = "UNSIGNED-PAYLOAD";

    let canonical_request = format!(
        "{method}\n{path}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
    );

    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{datetime}\n{credential_scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    );

    let signing_key = derive_signing_key(secret_key, date, region, "s3");
    let signature   = hex_encode(&hmac_sha256(&signing_key, string_to_sign.as_bytes()));

    format!(
        "{scheme}://{host}{path}?{canonical_query}&X-Amz-Signature={signature}"
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_hex_known() {
        // SHA256("") = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn hmac_sha256_known() {
        // RFC 2202 test vector: HMAC-SHA256("key", "The quick brown fox jumps over the lazy dog")
        let result = hmac_sha256(b"key", b"The quick brown fox jumps over the lazy dog");
        let hex = hex_encode(&result);
        assert_eq!(hex, "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8");
    }

    #[test]
    fn epoch_to_datetime_epoch() {
        let (dt, date) = epoch_to_datetime(0);
        assert_eq!(dt,   "19700101T000000Z");
        assert_eq!(date, "19700101");
    }

    #[test]
    fn epoch_to_datetime_known() {
        // 2024-01-15 12:30:45 UTC = 1705321845
        let (dt, date) = epoch_to_datetime(1705321845);
        assert_eq!(dt,   "20240115T123045Z");
        assert_eq!(date, "20240115");
    }

    #[test]
    fn epoch_to_datetime_leap_year() {
        // 2024-02-29 00:00:00 UTC = 1709164800
        let (dt, date) = epoch_to_datetime(1709164800);
        assert_eq!(dt,   "20240229T000000Z");
        assert_eq!(date, "20240229");
    }

    #[test]
    fn uri_encode_unreserved() {
        assert_eq!(uri_encode("hello-world_test.~ok", true), "hello-world_test.~ok");
    }

    #[test]
    fn uri_encode_special() {
        assert_eq!(uri_encode("hello world", true), "hello%20world");
        assert_eq!(uri_encode("a/b/c", true),  "a%2Fb%2Fc");
        assert_eq!(uri_encode("a/b/c", false), "a/b/c");
    }

    #[test]
    fn uri_encode_plus() {
        // '+' should be percent-encoded
        assert_eq!(uri_encode("a+b", true), "a%2Bb");
    }

    #[test]
    fn authorization_header_produces_aws4_prefix() {
        let (datetime, date) = epoch_to_datetime(1705320645);
        let auth = authorization_header(&AuthParams {
            method:        "PUT",
            host:          "s3.us-east-1.amazonaws.com",
            path:          "/my-bucket/test.txt",
            query:         "",
            extra_headers: &[],
            payload_hash:  &sha256_hex(b"hello"),
            datetime:      &datetime,
            date:          &date,
            region:        "us-east-1",
            access_key:    "AKID",
            secret_key:    "SECRET",
        });
        assert!(auth.starts_with("AWS4-HMAC-SHA256 Credential=AKID/"));
        assert!(auth.contains("SignedHeaders=host;x-amz-content-sha256;x-amz-date"));
        assert!(auth.contains("Signature="));
    }

    #[test]
    fn presigned_url_contains_required_params() {
        let (datetime, date) = epoch_to_datetime(1705320645);
        let url = presigned_url(
            "GET", "https",
            "s3.us-east-1.amazonaws.com",
            "/my-bucket/photo.jpg",
            "AKID", "SECRET", "us-east-1",
            &datetime, &date,
            3600, "",
        );
        assert!(url.contains("X-Amz-Algorithm=AWS4-HMAC-SHA256"));
        assert!(url.contains("X-Amz-Expires=3600"));
        assert!(url.contains("X-Amz-Signature="));
        assert!(url.starts_with("https://s3.us-east-1.amazonaws.com/my-bucket/photo.jpg?"));
    }

    #[test]
    fn derive_signing_key_deterministic() {
        let k1 = derive_signing_key("SECRET", "20240115", "us-east-1", "s3");
        let k2 = derive_signing_key("SECRET", "20240115", "us-east-1", "s3");
        assert_eq!(k1, k2);
        assert_eq!(k1.len(), 32);
    }

    #[test]
    fn hex_encode_correct() {
        assert_eq!(hex_encode(&[0x00, 0xde, 0xad, 0xbe, 0xef, 0xff]), "00deadbeefff");
    }
}
