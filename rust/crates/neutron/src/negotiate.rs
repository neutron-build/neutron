//! HTTP content negotiation based on the `Accept` header.
//!
//! Parses quality-weighted media ranges and selects the best match from a
//! set of offered content types. Also provides a [`Negotiate`] extractor.
//!
//! ```rust,ignore
//! let best = negotiate("text/html;q=0.9, application/json", &["text/html", "application/json"]);
//! assert_eq!(best, Some("application/json".to_string()));
//! ```

use std::fmt;
use std::str::FromStr;

use http::StatusCode;

use crate::extract::FromRequest;
use crate::handler::{IntoResponse, Request, Response};

// ---------------------------------------------------------------------------
// ContentType enum
// ---------------------------------------------------------------------------

/// Common content type variants with an escape hatch for arbitrary types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContentType {
    Json,
    Html,
    Xml,
    PlainText,
    OctetStream,
    FormUrlEncoded,
    Other(String),
}

impl ContentType {
    /// Return the MIME type string for this content type.
    pub fn as_str(&self) -> &str {
        match self {
            ContentType::Json => "application/json",
            ContentType::Html => "text/html",
            ContentType::Xml => "application/xml",
            ContentType::PlainText => "text/plain",
            ContentType::OctetStream => "application/octet-stream",
            ContentType::FormUrlEncoded => "application/x-www-form-urlencoded",
            ContentType::Other(s) => s.as_str(),
        }
    }
}

impl fmt::Display for ContentType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ContentType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim() {
            "application/json" => Ok(ContentType::Json),
            "text/html" => Ok(ContentType::Html),
            "application/xml" => Ok(ContentType::Xml),
            "text/plain" => Ok(ContentType::PlainText),
            "application/octet-stream" => Ok(ContentType::OctetStream),
            "application/x-www-form-urlencoded" => Ok(ContentType::FormUrlEncoded),
            other if other.contains('/') => Ok(ContentType::Other(other.to_string())),
            other => Err(format!("invalid content type: {other}")),
        }
    }
}

// ---------------------------------------------------------------------------
// Quality-value media type
// ---------------------------------------------------------------------------

/// A single media type entry from an Accept header, with its quality value.
#[derive(Debug, Clone)]
pub struct MediaRange {
    /// The full type, e.g. "text"
    type_: String,
    /// The subtype, e.g. "html"
    subtype: String,
    /// Quality value from 0 to 1000 (integer representation of 0.000 - 1.000).
    quality: u16,
}

impl MediaRange {
    /// Check whether this media range matches a concrete MIME type.
    fn matches(&self, mime: &str) -> bool {
        let (t, s) = match mime.split_once('/') {
            Some(pair) => pair,
            None => return false,
        };

        if self.type_ == "*" && self.subtype == "*" {
            return true;
        }
        if self.type_ == t && self.subtype == "*" {
            return true;
        }
        self.type_ == t && self.subtype == s
    }
}

// ---------------------------------------------------------------------------
// AcceptHeader
// ---------------------------------------------------------------------------

/// Parsed representation of the HTTP `Accept` header, with entries sorted by
/// quality value (highest first). Ties preserve original order.
#[derive(Debug, Clone)]
pub struct AcceptHeader {
    ranges: Vec<MediaRange>,
}

impl AcceptHeader {
    /// Parse an Accept header value.
    ///
    /// Example: `text/html;q=0.9, application/json;q=1.0, */*;q=0.1`
    pub fn parse(header: &str) -> Self {
        let mut ranges: Vec<MediaRange> = header
            .split(',')
            .filter_map(|entry| {
                let entry = entry.trim();
                if entry.is_empty() {
                    return None;
                }

                // Split on semicolons: first part is the media type, rest are params
                let mut parts = entry.split(';');
                let media = parts.next()?.trim();

                let (type_, subtype) = match media.split_once('/') {
                    Some((t, s)) => (t.trim().to_ascii_lowercase(), s.trim().to_ascii_lowercase()),
                    None => return None,
                };

                // Look for q= parameter
                let mut quality: u16 = 1000; // default q=1.0
                for param in parts {
                    let param = param.trim();
                    if let Some(q_str) = param.strip_prefix("q=").or_else(|| param.strip_prefix("Q=")) {
                        let q_str = q_str.trim();
                        if let Ok(q) = q_str.parse::<f64>() {
                            // Clamp to [0, 1] and convert to integer representation
                            let clamped = q.clamp(0.0, 1.0);
                            quality = (clamped * 1000.0).round() as u16;
                        }
                    }
                }

                Some(MediaRange {
                    type_,
                    subtype,
                    quality,
                })
            })
            .collect();

        // Sort by quality descending (stable sort preserves insertion order for ties)
        ranges.sort_by(|a, b| b.quality.cmp(&a.quality));

        AcceptHeader { ranges }
    }

    /// Return the parsed media ranges, sorted by quality (highest first).
    pub fn ranges(&self) -> &[MediaRange] {
        &self.ranges
    }

    /// Check whether the given MIME type is acceptable (quality > 0).
    pub fn accepts(&self, mime: &str) -> bool {
        for range in &self.ranges {
            if range.matches(mime) {
                return range.quality > 0;
            }
        }
        false
    }
}

// ---------------------------------------------------------------------------
// negotiate()
// ---------------------------------------------------------------------------

/// Find the best matching content type from a list of offered types, given an
/// `Accept` header string.
///
/// Returns `None` if no offered type is acceptable. When multiple offered types
/// match, the one matching the highest-quality Accept entry wins. Among ties
/// the first offered type wins.
///
/// If the Accept header is empty or absent, callers should pass `"*/*"` to
/// indicate that any type is acceptable.
pub fn negotiate(accept: &str, offered: &[&str]) -> Option<String> {
    let header = AcceptHeader::parse(accept);

    let mut best: Option<(u16, usize, &str)> = None; // (quality, offer_index, type)

    for (offer_idx, &offer) in offered.iter().enumerate() {
        for range in &header.ranges {
            if range.matches(offer) {
                if range.quality == 0 {
                    // Explicitly excluded
                    break;
                }
                let candidate = (range.quality, offer_idx, offer);
                best = Some(match best {
                    None => candidate,
                    Some(current) => {
                        // Higher quality wins; on tie, lower offer index wins
                        if candidate.0 > current.0
                            || (candidate.0 == current.0 && candidate.1 < current.1)
                        {
                            candidate
                        } else {
                            current
                        }
                    }
                });
                break; // First matching range for this offer is the most specific
            }
        }
    }

    best.map(|(_, _, t)| t.to_string())
}

// ---------------------------------------------------------------------------
// Negotiate extractor
// ---------------------------------------------------------------------------

/// Extractor that resolves the best matching content type from the request's
/// `Accept` header against a set of offered types.
///
/// If no `Accept` header is present, defaults to `*/*` (any type acceptable).
///
/// ```rust,ignore
/// async fn handler(Negotiate(content_type): Negotiate) -> impl IntoResponse {
///     match content_type {
///         ContentType::Json => /* ... */,
///         ContentType::Html => /* ... */,
///         _ => /* ... */,
///     }
/// }
/// ```
///
/// By default the extractor negotiates against the common types:
/// `application/json`, `text/html`, `application/xml`, `text/plain`.
/// For custom offered types, use [`Negotiate::with_offered`].
#[derive(Debug)]
pub struct Negotiate(pub ContentType);

/// The default set of offered types for the `Negotiate` extractor.
const DEFAULT_OFFERED: &[&str] = &[
    "application/json",
    "text/html",
    "application/xml",
    "text/plain",
];

impl Negotiate {
    /// Negotiate against a custom set of offered MIME types.
    pub fn with_offered(req: &Request, offered: &[&str]) -> Result<Self, Response> {
        let accept = req
            .headers()
            .get(http::header::ACCEPT)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("*/*");

        match negotiate(accept, offered) {
            Some(mime) => {
                let ct = mime
                    .parse::<ContentType>()
                    .unwrap_or(ContentType::Other(mime));
                Ok(Negotiate(ct))
            }
            None => Err((
                StatusCode::NOT_ACCEPTABLE,
                "No acceptable content type found",
            )
                .into_response()),
        }
    }
}

impl FromRequest for Negotiate {
    fn from_request(req: &Request) -> Result<Self, Response> {
        Negotiate::with_offered(req, DEFAULT_OFFERED)
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http::{HeaderMap, Method, Uri};

    /// Helper to build a Request with a given Accept header.
    fn request_with_accept(accept: &str) -> Request {
        let mut headers = HeaderMap::new();
        headers.insert(http::header::ACCEPT, accept.parse().unwrap());
        Request::new(
            Method::GET,
            Uri::from_static("/"),
            headers,
            Bytes::new(),
        )
    }

    /// Helper to build a Request with no Accept header.
    fn request_without_accept() -> Request {
        Request::new(
            Method::GET,
            Uri::from_static("/"),
            HeaderMap::new(),
            Bytes::new(),
        )
    }

    // -----------------------------------------------------------------------
    // 1. Parse quality values correctly
    // -----------------------------------------------------------------------

    #[test]
    fn parse_quality_values_correctly() {
        let header = AcceptHeader::parse("text/html;q=0.9, application/json;q=1.0");
        let ranges = header.ranges();
        assert_eq!(ranges.len(), 2);
        // First entry should be the highest quality
        assert_eq!(ranges[0].type_, "application");
        assert_eq!(ranges[0].subtype, "json");
        assert_eq!(ranges[0].quality, 1000);
        assert_eq!(ranges[1].type_, "text");
        assert_eq!(ranges[1].subtype, "html");
        assert_eq!(ranges[1].quality, 900);
    }

    // -----------------------------------------------------------------------
    // 2. Higher quality wins
    // -----------------------------------------------------------------------

    #[test]
    fn higher_quality_wins() {
        let result = negotiate(
            "text/html;q=0.5, application/json;q=0.9",
            &["text/html", "application/json"],
        );
        assert_eq!(result, Some("application/json".to_string()));
    }

    // -----------------------------------------------------------------------
    // 3. Wildcard matches anything
    // -----------------------------------------------------------------------

    #[test]
    fn wildcard_matches_anything() {
        let result = negotiate("*/*", &["application/json"]);
        assert_eq!(result, Some("application/json".to_string()));

        let result = negotiate("*/*", &["text/html"]);
        assert_eq!(result, Some("text/html".to_string()));

        let result = negotiate("*/*", &["image/png"]);
        assert_eq!(result, Some("image/png".to_string()));
    }

    // -----------------------------------------------------------------------
    // 4. text/* matches text/html
    // -----------------------------------------------------------------------

    #[test]
    fn partial_wildcard_matches_subtype() {
        let result = negotiate("text/*", &["text/html", "application/json"]);
        assert_eq!(result, Some("text/html".to_string()));

        // text/* should NOT match application/json
        let result = negotiate("text/*", &["application/json"]);
        assert_eq!(result, None);
    }

    // -----------------------------------------------------------------------
    // 5. No match returns None
    // -----------------------------------------------------------------------

    #[test]
    fn no_match_returns_none() {
        let result = negotiate("application/json", &["text/html", "text/plain"]);
        assert_eq!(result, None);
    }

    // -----------------------------------------------------------------------
    // 6. Missing Accept header defaults to */*
    // -----------------------------------------------------------------------

    #[test]
    fn missing_accept_defaults_to_wildcard() {
        let req = request_without_accept();
        let negotiate_result = Negotiate::from_request(&req);
        assert!(negotiate_result.is_ok(), "should succeed when Accept is missing");
        // The first default offered type should be chosen (application/json)
        let Negotiate(ct) = negotiate_result.unwrap_or_else(|_| panic!("expected Ok"));
        assert_eq!(ct, ContentType::Json);
    }

    // -----------------------------------------------------------------------
    // 7. Multiple offered types, best match selected
    // -----------------------------------------------------------------------

    #[test]
    fn multiple_offered_best_match_selected() {
        // Client prefers JSON over HTML
        let result = negotiate(
            "application/json;q=1.0, text/html;q=0.8, text/plain;q=0.5",
            &["text/plain", "text/html", "application/json"],
        );
        assert_eq!(result, Some("application/json".to_string()));

        // Client prefers HTML over JSON
        let result = negotiate(
            "text/html;q=1.0, application/json;q=0.7",
            &["application/json", "text/html"],
        );
        assert_eq!(result, Some("text/html".to_string()));

        // Equal quality: first offered type wins
        let result = negotiate(
            "text/html;q=1.0, application/json;q=1.0",
            &["application/json", "text/html"],
        );
        assert_eq!(result, Some("application/json".to_string()));
    }

    // -----------------------------------------------------------------------
    // 8. q=0 explicitly excludes
    // -----------------------------------------------------------------------

    #[test]
    fn q_zero_explicitly_excludes() {
        // JSON is explicitly excluded
        let result = negotiate(
            "application/json;q=0, text/html;q=1.0",
            &["application/json", "text/html"],
        );
        assert_eq!(result, Some("text/html".to_string()));

        // Only type offered is excluded
        let result = negotiate("application/json;q=0", &["application/json"]);
        assert_eq!(result, None);
    }

    // -----------------------------------------------------------------------
    // Additional edge-case tests
    // -----------------------------------------------------------------------

    #[test]
    fn default_quality_is_one() {
        // No q= means q=1.0
        let header = AcceptHeader::parse("application/json");
        assert_eq!(header.ranges()[0].quality, 1000);
    }

    #[test]
    fn content_type_round_trip() {
        let ct: ContentType = "application/json".parse().unwrap();
        assert_eq!(ct, ContentType::Json);
        assert_eq!(ct.as_str(), "application/json");
        assert_eq!(ct.to_string(), "application/json");
    }

    #[test]
    fn content_type_other_variant() {
        let ct: ContentType = "image/png".parse().unwrap();
        assert_eq!(ct, ContentType::Other("image/png".to_string()));
        assert_eq!(ct.as_str(), "image/png");
    }

    #[test]
    fn invalid_content_type_rejected() {
        let result = "not-a-mime".parse::<ContentType>();
        assert!(result.is_err());
    }

    #[test]
    fn accepts_method_respects_quality() {
        let header = AcceptHeader::parse("text/html;q=0.9, application/json;q=0");
        assert!(header.accepts("text/html"));
        assert!(!header.accepts("application/json")); // q=0 means excluded
    }

    #[test]
    fn negotiate_extractor_with_accept_header() {
        let req = request_with_accept("application/json");
        let result = Negotiate::from_request(&req);
        assert!(result.is_ok());
        let Negotiate(ct) = result.unwrap_or_else(|_| panic!("expected Ok"));
        assert_eq!(ct, ContentType::Json);
    }

    #[test]
    fn negotiate_extractor_not_acceptable() {
        let req = request_with_accept("image/png");
        let result = Negotiate::from_request(&req);
        assert!(result.is_err());
        let resp = match result {
            Err(r) => r,
            Ok(_) => panic!("expected Err"),
        };
        assert_eq!(resp.status(), StatusCode::NOT_ACCEPTABLE);
    }

    #[test]
    fn negotiate_extractor_custom_offered() {
        let req = request_with_accept("image/png;q=1.0, image/jpeg;q=0.8");
        let result = Negotiate::with_offered(&req, &["image/jpeg", "image/png"]);
        assert!(result.is_ok());
        let Negotiate(ct) = result.unwrap_or_else(|_| panic!("expected Ok"));
        assert_eq!(ct, ContentType::Other("image/png".to_string()));
    }

    #[test]
    fn case_insensitive_media_type() {
        let result = negotiate("Text/HTML", &["text/html"]);
        assert_eq!(result, Some("text/html".to_string()));
    }

    #[test]
    fn empty_accept_header() {
        // Empty string => no ranges => nothing matches
        let result = negotiate("", &["application/json"]);
        assert_eq!(result, None);
    }
}
