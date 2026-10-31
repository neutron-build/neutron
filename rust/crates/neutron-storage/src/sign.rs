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
    let k_secret = format!("AWS4{secret}");
    let k_date = hmac_sha256(k_secret.as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

/// Percent-encode a string for use in an AWS canonical query string or URL.
/// Encodes everything except unreserved characters: A-Z a-z 0-9 - _ . ~
pub fn uri_encode(s: &str, encode_slash: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
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
    let sec = (secs % 60) as u8;
    let min = ((secs / 60) % 60) as u8;
    let hour = ((secs / 3600) % 24) as u8;
    let days = secs / 86400;
    let (year, month, day) = days_to_ymd(days);
    let dt = format!("{year:04}{month:02}{day:02}T{hour:02}{min:02}{sec:02}Z");
    let date = dt[..8].to_string();
    (dt, date)
}

/// Convert days since 1970-01-01 to (year, month, day).
///
/// Howard Hinnant's `civil_from_days`, translated for an unsigned day count.
/// The previous handwritten decomposition narrowed the day-of-year to `u8`
/// before month conversion (every day-of-year ≥ 255 produced a wrong date —
/// on 2026-10-07 SigV4 signed `20260124`) and anchored its 400-year cycle at
/// 1970-01-01 as if it were a cycle boundary (1972-12-31 rendered as
/// 1973-01-01). Verified against an independent calendar implementation for
/// every fixture day 1970-01-01 through 2400-12-31 (see tests below).
fn days_to_ymd(z: u64) -> (u32, u8, u8) {
    let z = z as i64 + 719_468; // days since 0000-03-01
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    let y = yoe + era * 400 + (m <= 2) as i64;
    (y as u32, m as u8, d as u8)
}

// ---------------------------------------------------------------------------
// Authorization-header signing
// ---------------------------------------------------------------------------

/// Parameters for signing a request via the Authorization header.
pub struct AuthParams<'a> {
    pub method: &'a str,
    pub host: &'a str,
    /// URL-encoded path (e.g. `/bucket/path/to/key`).
    pub path: &'a str,
    /// Pre-sorted, already-encoded canonical query string (empty if none).
    pub query: &'a str,
    /// Additional headers to sign, sorted by name (lowercase).
    /// `host`, `x-amz-content-sha256`, and `x-amz-date` are always added.
    pub extra_headers: &'a [(&'a str, &'a str)],
    pub payload_hash: &'a str,
    pub datetime: &'a str,
    pub date: &'a str,
    pub region: &'a str,
    pub access_key: &'a str,
    pub secret_key: &'a str,
}

/// Returns the `Authorization` header value and the sorted signed-header list
/// (needed for building the actual request headers).
pub fn authorization_header(p: &AuthParams<'_>) -> String {
    // Fixed headers that are always signed
    let mut headers: Vec<(&str, String)> = vec![
        ("host", p.host.to_string()),
        ("x-amz-content-sha256", p.payload_hash.to_string()),
        ("x-amz-date", p.datetime.to_string()),
    ];
    for (k, v) in p.extra_headers {
        headers.push((k, v.to_string()));
    }
    headers.sort_by_key(|(k, _)| *k);

    let canonical_headers: String = headers
        .iter()
        .map(|(k, v)| format!("{}:{}\n", k, v.trim()))
        .collect();

    let signed_headers: String = headers
        .iter()
        .map(|(k, _)| *k)
        .collect::<Vec<_>>()
        .join(";");

    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        p.method, p.path, p.query, canonical_headers, signed_headers, p.payload_hash
    );

    let credential_scope = format!("{}/{}/s3/aws4_request", p.date, p.region);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        p.datetime,
        credential_scope,
        sha256_hex(canonical_request.as_bytes())
    );

    let signing_key = derive_signing_key(p.secret_key, p.date, p.region, "s3");
    let signature = hex_encode(&hmac_sha256(&signing_key, string_to_sign.as_bytes()));

    format!(
        "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
        p.access_key, credential_scope, signed_headers, signature
    )
}

// ---------------------------------------------------------------------------
// Presigned URL signing
// ---------------------------------------------------------------------------

/// Build a presigned URL (signature in query parameters, no Authorization header).
#[allow(clippy::too_many_arguments)]
pub fn presigned_url(
    method: &str,
    scheme: &str,
    host: &str,
    path: &str,
    access_key: &str,
    secret_key: &str,
    region: &str,
    datetime: &str,
    date: &str,
    expires: u64,
    extra_query: &str, // additional query params already canonical-encoded, or ""
) -> String {
    let credential_scope = format!("{date}/{region}/s3/aws4_request");
    let credential = format!("{access_key}/{credential_scope}");

    // Build canonical query string (must be sorted)
    let mut qparams: Vec<(String, String)> = vec![
        (
            "X-Amz-Algorithm".to_string(),
            "AWS4-HMAC-SHA256".to_string(),
        ),
        (
            "X-Amz-Credential".to_string(),
            uri_encode(&credential, true),
        ),
        ("X-Amz-Date".to_string(), datetime.to_string()),
        ("X-Amz-Expires".to_string(), expires.to_string()),
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

    let canonical_query: String = qparams
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");

    let canonical_headers = format!("host:{host}\n");
    let signed_headers = "host";
    let payload_hash = "UNSIGNED-PAYLOAD";

    let canonical_request = format!(
        "{method}\n{path}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
    );

    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{datetime}\n{credential_scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    );

    let signing_key = derive_signing_key(secret_key, date, region, "s3");
    let signature = hex_encode(&hmac_sha256(&signing_key, string_to_sign.as_bytes()));

    format!("{scheme}://{host}{path}?{canonical_query}&X-Amz-Signature={signature}")
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
        assert_eq!(
            hex,
            "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"
        );
    }

    #[test]
    fn epoch_to_datetime_epoch() {
        let (dt, date) = epoch_to_datetime(0);
        assert_eq!(dt, "19700101T000000Z");
        assert_eq!(date, "19700101");
    }

    #[test]
    fn epoch_to_datetime_known() {
        // 2024-01-15 12:30:45 UTC = 1705321845
        let (dt, date) = epoch_to_datetime(1705321845);
        assert_eq!(dt, "20240115T123045Z");
        assert_eq!(date, "20240115");
    }

    #[test]
    fn epoch_to_datetime_leap_year() {
        // 2024-02-29 00:00:00 UTC = 1709164800
        let (dt, date) = epoch_to_datetime(1709164800);
        assert_eq!(dt, "20240229T000000Z");
        assert_eq!(date, "20240229");
    }

    /// Regression (RS-14): the old handwritten decomposition narrowed the
    /// day-of-year to u8 before month conversion (days ≥ 255 rendered wrong
    /// dates — the audit date 2026-10-07 signed as 20260124) and anchored its
    /// 400-year cycle at 1970-01-01 (1972-12-31 → 1973-01-01). These fixtures
    /// were generated by an independent implementation (Python `datetime`,
    /// proleptic Gregorian) and cover the u8 boundary (day-of-year 254-258),
    /// leap/century/400-year edges, and both endpoints through 2400.
    #[test]
    fn days_to_ymd_matches_independent_calendar() {
        const FIXTURES: &[(u64, u32, u8, u8)] = &[
            (0, 1970, 1, 1),
            (254, 1970, 9, 12),
            (255, 1970, 9, 13),
            (256, 1970, 9, 14),
            (257, 1970, 9, 15),
            (258, 1970, 9, 16),
            (364, 1970, 12, 31),
            (365, 1971, 1, 1),
            (366, 1971, 1, 2),
            (984, 1972, 9, 11),
            (985, 1972, 9, 12),
            (986, 1972, 9, 13),
            (987, 1972, 9, 14),
            (988, 1972, 9, 15),
            (1094, 1972, 12, 30),
            (1095, 1972, 12, 31),
            (1096, 1973, 1, 1),
            (2249, 1976, 2, 28),
            (2250, 1976, 2, 29),
            (2251, 1976, 3, 1),
            (2445, 1976, 9, 11),
            (2446, 1976, 9, 12),
            (2447, 1976, 9, 13),
            (2448, 1976, 9, 14),
            (2449, 1976, 9, 15),
            (2555, 1976, 12, 30),
            (2556, 1976, 12, 31),
            (2557, 1977, 1, 1),
            (6098, 1986, 9, 12),
            (6099, 1986, 9, 13),
            (6100, 1986, 9, 14),
            (6101, 1986, 9, 15),
            (6102, 1986, 9, 16),
            (6208, 1986, 12, 31),
            (6209, 1987, 1, 1),
            (6210, 1987, 1, 2),
            (11015, 2000, 2, 28),
            (11016, 2000, 2, 29),
            (11017, 2000, 3, 1),
            (11211, 2000, 9, 11),
            (11212, 2000, 9, 12),
            (11213, 2000, 9, 13),
            (11214, 2000, 9, 14),
            (11215, 2000, 9, 15),
            (11321, 2000, 12, 30),
            (11322, 2000, 12, 31),
            (11323, 2001, 1, 1),
            (20454, 2026, 1, 1),
            (20463, 2026, 1, 10),
            (20472, 2026, 1, 19),
            (20481, 2026, 1, 28),
            (20490, 2026, 2, 6),
            (20499, 2026, 2, 15),
            (20508, 2026, 2, 24),
            (20517, 2026, 3, 5),
            (20526, 2026, 3, 14),
            (20535, 2026, 3, 23),
            (20544, 2026, 4, 1),
            (20553, 2026, 4, 10),
            (20562, 2026, 4, 19),
            (20571, 2026, 4, 28),
            (20580, 2026, 5, 7),
            (20589, 2026, 5, 16),
            (20598, 2026, 5, 25),
            (20607, 2026, 6, 3),
            (20616, 2026, 6, 12),
            (20625, 2026, 6, 21),
            (20634, 2026, 6, 30),
            (20643, 2026, 7, 9),
            (20652, 2026, 7, 18),
            (20661, 2026, 7, 27),
            (20670, 2026, 8, 5),
            (20679, 2026, 8, 14),
            (20688, 2026, 8, 23),
            (20697, 2026, 9, 1),
            (20706, 2026, 9, 10),
            (20708, 2026, 9, 12),
            (20709, 2026, 9, 13),
            (20710, 2026, 9, 14),
            (20711, 2026, 9, 15),
            (20712, 2026, 9, 16),
            (20715, 2026, 9, 19),
            (20724, 2026, 9, 28),
            (20733, 2026, 10, 7),
            (20742, 2026, 10, 16),
            (20751, 2026, 10, 25),
            (20760, 2026, 11, 3),
            (20769, 2026, 11, 12),
            (20778, 2026, 11, 21),
            (20787, 2026, 11, 30),
            (20796, 2026, 12, 9),
            (20805, 2026, 12, 18),
            (20814, 2026, 12, 27),
            (20818, 2026, 12, 31),
            (20819, 2027, 1, 1),
            (20820, 2027, 1, 2),
            (21184, 2028, 1, 1),
            (21193, 2028, 1, 10),
            (21202, 2028, 1, 19),
            (21211, 2028, 1, 28),
            (21220, 2028, 2, 6),
            (21229, 2028, 2, 15),
            (21238, 2028, 2, 24),
            (21247, 2028, 3, 4),
            (21256, 2028, 3, 13),
            (21265, 2028, 3, 22),
            (21274, 2028, 3, 31),
            (21283, 2028, 4, 9),
            (21292, 2028, 4, 18),
            (21301, 2028, 4, 27),
            (21310, 2028, 5, 6),
            (21319, 2028, 5, 15),
            (21328, 2028, 5, 24),
            (21337, 2028, 6, 2),
            (21346, 2028, 6, 11),
            (21355, 2028, 6, 20),
            (21364, 2028, 6, 29),
            (21373, 2028, 7, 8),
            (21382, 2028, 7, 17),
            (21391, 2028, 7, 26),
            (21400, 2028, 8, 4),
            (21409, 2028, 8, 13),
            (21418, 2028, 8, 22),
            (21427, 2028, 8, 31),
            (21436, 2028, 9, 9),
            (21445, 2028, 9, 18),
            (21454, 2028, 9, 27),
            (21463, 2028, 10, 6),
            (21472, 2028, 10, 15),
            (21481, 2028, 10, 24),
            (21490, 2028, 11, 2),
            (21499, 2028, 11, 11),
            (21508, 2028, 11, 20),
            (21517, 2028, 11, 29),
            (21526, 2028, 12, 8),
            (21535, 2028, 12, 17),
            (21544, 2028, 12, 26),
            (25091, 2038, 9, 12),
            (25092, 2038, 9, 13),
            (25093, 2038, 9, 14),
            (25094, 2038, 9, 15),
            (25095, 2038, 9, 16),
            (25201, 2038, 12, 31),
            (25202, 2039, 1, 1),
            (25203, 2039, 1, 2),
            (47371, 2099, 9, 12),
            (47372, 2099, 9, 13),
            (47373, 2099, 9, 14),
            (47374, 2099, 9, 15),
            (47375, 2099, 9, 16),
            (47481, 2099, 12, 31),
            (47482, 2100, 1, 1),
            (47483, 2100, 1, 2),
            (47540, 2100, 2, 28),
            (47541, 2100, 3, 1),
            (47736, 2100, 9, 12),
            (47737, 2100, 9, 13),
            (47738, 2100, 9, 14),
            (47739, 2100, 9, 15),
            (47740, 2100, 9, 16),
            (47846, 2100, 12, 31),
            (47847, 2101, 1, 1),
            (47848, 2101, 1, 2),
            (49196, 2104, 9, 11),
            (49197, 2104, 9, 12),
            (49198, 2104, 9, 13),
            (49199, 2104, 9, 14),
            (49200, 2104, 9, 15),
            (49306, 2104, 12, 30),
            (49307, 2104, 12, 31),
            (49308, 2105, 1, 1),
            (157113, 2400, 2, 29),
            (157308, 2400, 9, 11),
            (157309, 2400, 9, 12),
            (157310, 2400, 9, 13),
            (157311, 2400, 9, 14),
            (157312, 2400, 9, 15),
            (157418, 2400, 12, 30),
            (157419, 2400, 12, 31),
            (157420, 2401, 1, 1),
        ];
        for &(days, y, m, d) in FIXTURES {
            assert_eq!(days_to_ymd(days), (y, m, d), "days={days}");
        }
    }

    #[test]
    fn audit_date_2026_10_07_renders_correctly() {
        // The concrete SigV4 break from the audit: day-of-year 280 used to
        // narrow to 24 and sign 20260124.
        let (dt, _date) = epoch_to_datetime(1_791_331_200); // 2026-10-07T00:00:00Z
        assert_eq!(dt, "20261007T000000Z");
    }

    #[test]
    fn uri_encode_unreserved() {
        assert_eq!(
            uri_encode("hello-world_test.~ok", true),
            "hello-world_test.~ok"
        );
    }

    #[test]
    fn uri_encode_special() {
        assert_eq!(uri_encode("hello world", true), "hello%20world");
        assert_eq!(uri_encode("a/b/c", true), "a%2Fb%2Fc");
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
            method: "PUT",
            host: "s3.us-east-1.amazonaws.com",
            path: "/my-bucket/test.txt",
            query: "",
            extra_headers: &[],
            payload_hash: &sha256_hex(b"hello"),
            datetime: &datetime,
            date: &date,
            region: "us-east-1",
            access_key: "AKID",
            secret_key: "SECRET",
        });
        assert!(auth.starts_with("AWS4-HMAC-SHA256 Credential=AKID/"));
        assert!(auth.contains("SignedHeaders=host;x-amz-content-sha256;x-amz-date"));
        assert!(auth.contains("Signature="));
    }

    #[test]
    fn presigned_url_contains_required_params() {
        let (datetime, date) = epoch_to_datetime(1705320645);
        let url = presigned_url(
            "GET",
            "https",
            "s3.us-east-1.amazonaws.com",
            "/my-bucket/photo.jpg",
            "AKID",
            "SECRET",
            "us-east-1",
            &datetime,
            &date,
            3600,
            "",
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
        assert_eq!(
            hex_encode(&[0x00, 0xde, 0xad, 0xbe, 0xef, 0xff]),
            "00deadbeefff"
        );
    }
}
