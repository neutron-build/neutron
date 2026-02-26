//! Request validation system.
//!
//! Provides a [`Validate`] trait and [`Validated<T>`] extractor wrapper for
//! type-safe request validation with structured JSON error responses.
//!
//! # Example
//!
//! ```rust,ignore
//! use neutron::prelude::*;
//! use neutron::validate::{Validate, Validated, ValidationErrors};
//! use serde::Deserialize;
//!
//! #[derive(Deserialize)]
//! struct CreateUser {
//!     name: String,
//!     email: String,
//!     age: u32,
//! }
//!
//! impl Validate for CreateUser {
//!     fn validate(&self) -> Result<(), ValidationErrors> {
//!         let mut e = ValidationErrors::new();
//!         e.required("name", &self.name);
//!         e.max_length("name", &self.name, 100);
//!         e.email("email", &self.email);
//!         e.range("age", self.age, 13, 150);
//!         e.into_result()
//!     }
//! }
//!
//! let router = Router::new()
//!     .post("/users", |Validated(Json(user)): Validated<Json<CreateUser>>| async move {
//!         format!("Hello, {}!", user.name)
//!     });
//! ```
//!
//! ## Error Response
//!
//! When validation fails, a 422 Unprocessable Entity response is returned
//! with a JSON body:
//!
//! ```json
//! {
//!   "error": "Validation failed",
//!   "fields": {
//!     "name": [{ "code": "required", "message": "name is required" }],
//!     "email": [{ "code": "email", "message": "email must be a valid email address" }]
//!   }
//! }
//! ```

use std::collections::HashMap;
use std::fmt;

use http::StatusCode;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::extract::{FromRequest, Query};
use crate::handler::{Body, IntoResponse, Json, Request, Response};

// ---------------------------------------------------------------------------
// Validate trait
// ---------------------------------------------------------------------------

/// Trait for types that can be validated after deserialization.
///
/// Implement this on your request DTOs to enable validation via the
/// [`Validated`] extractor wrapper.
pub trait Validate {
    /// Validate the value, returning structured errors if invalid.
    fn validate(&self) -> Result<(), ValidationErrors>;
}

// ---------------------------------------------------------------------------
// FieldError
// ---------------------------------------------------------------------------

/// A single field-level validation error.
#[derive(Debug, Clone, Serialize)]
pub struct FieldError {
    /// Machine-readable error code (e.g. `"required"`, `"max_length"`, `"email"`).
    pub code: String,
    /// Human-readable error message.
    pub message: String,
}

// ---------------------------------------------------------------------------
// ValidationErrors
// ---------------------------------------------------------------------------

/// Structured collection of validation errors, grouped by field name.
///
/// Provides helper methods for common validation rules:
///
/// ```rust,ignore
/// let mut e = ValidationErrors::new();
/// e.required("name", &self.name);
/// e.email("email", &self.email);
/// e.range("age", self.age, 13, 150);
/// e.into_result()
/// ```
#[derive(Debug, Clone)]
pub struct ValidationErrors {
    fields: HashMap<String, Vec<FieldError>>,
}

impl Default for ValidationErrors {
    fn default() -> Self {
        Self::new()
    }
}

impl ValidationErrors {
    /// Create an empty error collection.
    pub fn new() -> Self {
        Self {
            fields: HashMap::new(),
        }
    }

    /// Add a validation error for a specific field.
    pub fn add(&mut self, field: &str, code: &str, message: impl Into<String>) {
        self.fields
            .entry(field.to_string())
            .or_default()
            .push(FieldError {
                code: code.to_string(),
                message: message.into(),
            });
    }

    /// Returns `true` if there are no validation errors.
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Convert into `Result<(), Self>` — `Ok(())` if empty, `Err(self)` otherwise.
    pub fn into_result(self) -> Result<(), Self> {
        if self.is_empty() {
            Ok(())
        } else {
            Err(self)
        }
    }

    /// Get all field errors as a map.
    pub fn fields(&self) -> &HashMap<String, Vec<FieldError>> {
        &self.fields
    }

    /// Get errors for a specific field.
    pub fn field_errors(&self, field: &str) -> Option<&[FieldError]> {
        self.fields.get(field).map(|v| v.as_slice())
    }

    /// Total number of errors across all fields.
    pub fn error_count(&self) -> usize {
        self.fields.values().map(|v| v.len()).sum()
    }

    // -----------------------------------------------------------------------
    // Validation helpers
    // -----------------------------------------------------------------------

    /// Require a non-empty string (after trimming whitespace).
    pub fn required(&mut self, field: &str, value: &str) {
        if value.trim().is_empty() {
            self.add(field, "required", format!("{field} is required"));
        }
    }

    /// Require an `Option` to be `Some`.
    pub fn required_some<T>(&mut self, field: &str, value: &Option<T>) {
        if value.is_none() {
            self.add(field, "required", format!("{field} is required"));
        }
    }

    /// Require string length >= `min`.
    pub fn min_length(&mut self, field: &str, value: &str, min: usize) {
        if value.len() < min {
            self.add(
                field,
                "min_length",
                format!("{field} must be at least {min} characters"),
            );
        }
    }

    /// Require string length <= `max`.
    pub fn max_length(&mut self, field: &str, value: &str, max: usize) {
        if value.len() > max {
            self.add(
                field,
                "max_length",
                format!("{field} must be at most {max} characters"),
            );
        }
    }

    /// Require string length in `[min, max]`.
    pub fn length(&mut self, field: &str, value: &str, min: usize, max: usize) {
        let len = value.len();
        if len < min || len > max {
            self.add(
                field,
                "length",
                format!("{field} must be between {min} and {max} characters"),
            );
        }
    }

    /// Require a numeric value in `[min, max]` (inclusive).
    pub fn range<T: PartialOrd + fmt::Display>(
        &mut self,
        field: &str,
        value: T,
        min: T,
        max: T,
    ) {
        if value < min || value > max {
            self.add(
                field,
                "range",
                format!("{field} must be between {min} and {max}"),
            );
        }
    }

    /// Require a valid email address.
    pub fn email(&mut self, field: &str, value: &str) {
        if !is_email(value) {
            self.add(
                field,
                "email",
                format!("{field} must be a valid email address"),
            );
        }
    }

    /// Require a valid URL (http:// or https://).
    pub fn url(&mut self, field: &str, value: &str) {
        if !is_url(value) {
            self.add(field, "url", format!("{field} must be a valid URL"));
        }
    }

    /// Require only ASCII alphanumeric characters.
    pub fn alphanumeric(&mut self, field: &str, value: &str) {
        if !value.chars().all(|c| c.is_ascii_alphanumeric()) {
            self.add(
                field,
                "alphanumeric",
                format!("{field} must contain only letters and numbers"),
            );
        }
    }

    /// Require value to be one of the allowed values.
    pub fn one_of<T: PartialEq + fmt::Display>(
        &mut self,
        field: &str,
        value: &T,
        allowed: &[T],
    ) {
        if !allowed.iter().any(|a| a == value) {
            let list: Vec<_> = allowed.iter().map(|v| v.to_string()).collect();
            self.add(
                field,
                "one_of",
                format!("{field} must be one of: {}", list.join(", ")),
            );
        }
    }

    /// Require string to contain a substring.
    pub fn contains_str(&mut self, field: &str, value: &str, substr: &str) {
        if !value.contains(substr) {
            self.add(
                field,
                "contains",
                format!("{field} must contain '{substr}'"),
            );
        }
    }

    /// Custom validation with a boolean predicate.
    ///
    /// Adds the error only if `valid` is `false`.
    pub fn custom(&mut self, field: &str, code: &str, message: impl Into<String>, valid: bool) {
        if !valid {
            self.add(field, code, message);
        }
    }

    /// Validate with a pattern function (useful for regex or other matchers).
    ///
    /// Adds the error only if `pattern_fn` returns `false`.
    pub fn matches(
        &mut self,
        field: &str,
        value: &str,
        pattern_fn: impl FnOnce(&str) -> bool,
        message: impl Into<String>,
    ) {
        if !pattern_fn(value) {
            self.add(field, "pattern", message);
        }
    }
}

impl fmt::Display for ValidationErrors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Validation failed: {} error(s)", self.error_count())
    }
}

impl std::error::Error for ValidationErrors {}

impl IntoResponse for ValidationErrors {
    fn into_response(self) -> Response {
        #[derive(Serialize)]
        struct ErrorBody<'a> {
            error: &'a str,
            fields: &'a HashMap<String, Vec<FieldError>>,
        }

        let body = ErrorBody {
            error: "Validation failed",
            fields: &self.fields,
        };

        let json = serde_json::to_vec(&body)
            .unwrap_or_else(|_| br#"{"error":"Validation failed"}"#.to_vec());

        http::Response::builder()
            .status(StatusCode::UNPROCESSABLE_ENTITY)
            .header("content-type", "application/json")
            .body(Body::full(json))
            .unwrap()
    }
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Check if a string looks like a valid email address.
///
/// This performs basic structural validation (local@domain.tld).
/// For strict RFC 5322 compliance, use a dedicated email validation library.
pub fn is_email(value: &str) -> bool {
    let value = value.trim();
    if value.is_empty() {
        return false;
    }
    let Some((local, domain)) = value.split_once('@') else {
        return false;
    };
    if local.is_empty() || domain.is_empty() {
        return false;
    }
    if local.contains(' ') || domain.contains(' ') {
        return false;
    }
    // Local part: no leading/trailing dots, no consecutive dots
    if local.starts_with('.') || local.ends_with('.') || local.contains("..") {
        return false;
    }
    if domain.starts_with('.') || domain.ends_with('.') {
        return false;
    }
    if !domain.contains('.') {
        return false;
    }
    // Domain labels can't be empty (no consecutive dots)
    if domain.contains("..") {
        return false;
    }
    true
}

/// Check if a string looks like a valid URL (http or https).
pub fn is_url(value: &str) -> bool {
    let value = value.trim();
    let rest = if let Some(r) = value.strip_prefix("https://") {
        r
    } else if let Some(r) = value.strip_prefix("http://") {
        r
    } else {
        return false;
    };
    // Must have a host with at least one dot
    if rest.is_empty() {
        return false;
    }
    // Extract host part (before any / ? #)
    let host = rest.split(&['/', '?', '#'][..]).next().unwrap_or("");
    !host.is_empty() && host.contains('.')
}

// ---------------------------------------------------------------------------
// Validated<T> extractor
// ---------------------------------------------------------------------------

/// Extractor wrapper that validates data after deserialization.
///
/// Wraps another extractor (like [`Json<T>`] or [`Query<T>`]) and calls
/// `T::validate()` after successful extraction. Returns 422 Unprocessable
/// Entity with structured JSON errors if validation fails.
///
/// # Example
///
/// ```rust,ignore
/// use neutron::validate::{Validate, Validated, ValidationErrors};
///
/// async fn create(Validated(Json(data)): Validated<Json<CreateUser>>) -> String {
///     // `data` is guaranteed valid here
///     format!("Created {}", data.name)
/// }
/// ```
pub struct Validated<T>(pub T);

impl<T: DeserializeOwned + Validate + Send + 'static> FromRequest for Validated<Json<T>> {
    fn from_request(req: &Request) -> Result<Self, Response> {
        let Json(inner) = Json::<T>::from_request(req)?;
        inner.validate().map_err(|e| e.into_response())?;
        Ok(Validated(Json(inner)))
    }
}

impl<T: DeserializeOwned + Validate + Send + 'static> FromRequest for Validated<Query<T>> {
    fn from_request(req: &Request) -> Result<Self, Response> {
        let Query(inner) = Query::<T>::from_request(req)?;
        inner.validate().map_err(|e| e.into_response())?;
        Ok(Validated(Query(inner)))
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::router::Router;
    use crate::testing::TestClient;
    use serde::Deserialize;

    // -----------------------------------------------------------------------
    // Test DTO
    // -----------------------------------------------------------------------

    #[derive(Deserialize)]
    struct CreateUser {
        name: String,
        email: String,
        age: u32,
    }

    impl Validate for CreateUser {
        fn validate(&self) -> Result<(), ValidationErrors> {
            let mut e = ValidationErrors::new();
            e.required("name", &self.name);
            e.max_length("name", &self.name, 100);
            e.email("email", &self.email);
            e.range("age", self.age, 13, 150);
            e.into_result()
        }
    }

    #[derive(Deserialize)]
    struct SearchParams {
        q: String,
        page: Option<u32>,
    }

    impl Validate for SearchParams {
        fn validate(&self) -> Result<(), ValidationErrors> {
            let mut e = ValidationErrors::new();
            e.required("q", &self.q);
            e.max_length("q", &self.q, 200);
            if let Some(page) = self.page {
                e.range("page", page, 1, 1000);
            }
            e.into_result()
        }
    }

    fn json_client() -> TestClient {
        TestClient::new(
            Router::new()
                .post(
                    "/users",
                    |Validated(Json(user)): Validated<Json<CreateUser>>| async move {
                        format!("Hello, {}!", user.name)
                    },
                )
                .get(
                    "/search",
                    |Validated(Query(params)): Validated<Query<SearchParams>>| async move {
                        format!("Searching for: {}", params.q)
                    },
                ),
        )
    }

    // -----------------------------------------------------------------------
    // Integration tests — Validated<Json<T>>
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn valid_json_passes() {
        let client = json_client();
        let resp = client
            .post("/users")
            .header("content-type", "application/json")
            .body(r#"{"name":"Alice","email":"alice@example.com","age":30}"#)
            .send()
            .await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.text().await, "Hello, Alice!");
    }

    #[tokio::test]
    async fn invalid_json_returns_422() {
        let client = json_client();
        let resp = client
            .post("/users")
            .header("content-type", "application/json")
            .body(r#"{"name":"","email":"not-an-email","age":5}"#)
            .send()
            .await;
        assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);

        let body = resp.text().await;
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["error"], "Validation failed");
        assert!(parsed["fields"]["name"].is_array());
        assert!(parsed["fields"]["email"].is_array());
        assert!(parsed["fields"]["age"].is_array());
    }

    #[tokio::test]
    async fn single_field_error() {
        let client = json_client();
        let resp = client
            .post("/users")
            .header("content-type", "application/json")
            .body(r#"{"name":"Alice","email":"bad","age":30}"#)
            .send()
            .await;
        assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);

        let body = resp.text().await;
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
        // Only email should fail
        assert!(parsed["fields"]["email"].is_array());
        assert!(parsed["fields"].get("name").is_none());
        assert!(parsed["fields"].get("age").is_none());
    }

    #[tokio::test]
    async fn malformed_json_returns_400_not_422() {
        let client = json_client();
        let resp = client
            .post("/users")
            .header("content-type", "application/json")
            .body("not json at all")
            .send()
            .await;
        // Malformed JSON hits the Json extractor error (400), not validation (422)
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    // -----------------------------------------------------------------------
    // Integration tests — Validated<Query<T>>
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn valid_query_passes() {
        let client = json_client();
        let resp = client.get("/search?q=rust&page=1").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.text().await, "Searching for: rust");
    }

    #[tokio::test]
    async fn invalid_query_returns_422() {
        let client = json_client();
        let resp = client.get("/search?q=&page=0").send().await;
        assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);

        let body = resp.text().await;
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["error"], "Validation failed");
        assert!(parsed["fields"]["q"].is_array());
        assert!(parsed["fields"]["page"].is_array());
    }

    // -----------------------------------------------------------------------
    // Unit tests — ValidationErrors helpers
    // -----------------------------------------------------------------------

    #[test]
    fn empty_errors_is_ok() {
        let e = ValidationErrors::new();
        assert!(e.is_empty());
        assert_eq!(e.error_count(), 0);
        assert!(e.into_result().is_ok());
    }

    #[test]
    fn non_empty_errors_is_err() {
        let mut e = ValidationErrors::new();
        e.add("field", "code", "message");
        assert!(!e.is_empty());
        assert_eq!(e.error_count(), 1);
        assert!(e.into_result().is_err());
    }

    #[test]
    fn multiple_errors_per_field() {
        let mut e = ValidationErrors::new();
        e.add("name", "required", "name is required");
        e.add("name", "min_length", "name too short");
        assert_eq!(e.error_count(), 2);
        assert_eq!(e.field_errors("name").unwrap().len(), 2);
    }

    #[test]
    fn required_catches_empty() {
        let mut e = ValidationErrors::new();
        e.required("name", "");
        assert!(!e.is_empty());
    }

    #[test]
    fn required_catches_whitespace_only() {
        let mut e = ValidationErrors::new();
        e.required("name", "   ");
        assert!(!e.is_empty());
    }

    #[test]
    fn required_passes_non_empty() {
        let mut e = ValidationErrors::new();
        e.required("name", "Alice");
        assert!(e.is_empty());
    }

    #[test]
    fn required_some_catches_none() {
        let mut e = ValidationErrors::new();
        e.required_some::<String>("field", &None);
        assert!(!e.is_empty());
    }

    #[test]
    fn required_some_passes_some() {
        let mut e = ValidationErrors::new();
        e.required_some("field", &Some(42));
        assert!(e.is_empty());
    }

    #[test]
    fn min_length_catches_short() {
        let mut e = ValidationErrors::new();
        e.min_length("pw", "ab", 3);
        assert!(!e.is_empty());
        assert_eq!(e.field_errors("pw").unwrap()[0].code, "min_length");
    }

    #[test]
    fn min_length_passes_exact() {
        let mut e = ValidationErrors::new();
        e.min_length("pw", "abc", 3);
        assert!(e.is_empty());
    }

    #[test]
    fn max_length_catches_long() {
        let mut e = ValidationErrors::new();
        e.max_length("name", "abcdef", 5);
        assert!(!e.is_empty());
        assert_eq!(e.field_errors("name").unwrap()[0].code, "max_length");
    }

    #[test]
    fn max_length_passes_exact() {
        let mut e = ValidationErrors::new();
        e.max_length("name", "abcde", 5);
        assert!(e.is_empty());
    }

    #[test]
    fn length_validates_range() {
        let mut e = ValidationErrors::new();
        e.length("code", "ab", 3, 10);
        assert!(!e.is_empty());

        let mut e = ValidationErrors::new();
        e.length("code", "abcdefghijk", 3, 10);
        assert!(!e.is_empty());

        let mut e = ValidationErrors::new();
        e.length("code", "abcde", 3, 10);
        assert!(e.is_empty());
    }

    #[test]
    fn range_catches_below() {
        let mut e = ValidationErrors::new();
        e.range("age", 5, 13, 150);
        assert!(!e.is_empty());
        assert_eq!(e.field_errors("age").unwrap()[0].code, "range");
    }

    #[test]
    fn range_catches_above() {
        let mut e = ValidationErrors::new();
        e.range("age", 200, 13, 150);
        assert!(!e.is_empty());
    }

    #[test]
    fn range_passes_within() {
        let mut e = ValidationErrors::new();
        e.range("age", 25u32, 13, 150);
        assert!(e.is_empty());
    }

    #[test]
    fn range_passes_boundary() {
        let mut e = ValidationErrors::new();
        e.range("age", 13u32, 13, 150);
        assert!(e.is_empty());

        let mut e = ValidationErrors::new();
        e.range("age", 150u32, 13, 150);
        assert!(e.is_empty());
    }

    // -----------------------------------------------------------------------
    // Unit tests — email validation
    // -----------------------------------------------------------------------

    #[test]
    fn email_valid() {
        assert!(is_email("user@example.com"));
        assert!(is_email("user.name@example.com"));
        assert!(is_email("user+tag@example.co.uk"));
        assert!(is_email("a@b.c"));
    }

    #[test]
    fn email_invalid() {
        assert!(!is_email(""));
        assert!(!is_email("user"));
        assert!(!is_email("@example.com"));
        assert!(!is_email("user@"));
        assert!(!is_email("user@.com"));
        assert!(!is_email("user@com."));
        assert!(!is_email("user@com"));
        assert!(!is_email("user @example.com"));
        assert!(!is_email("user@example..com"));
    }

    #[test]
    fn email_validator_on_errors() {
        let mut e = ValidationErrors::new();
        e.email("email", "bad");
        assert!(!e.is_empty());

        let mut e = ValidationErrors::new();
        e.email("email", "good@example.com");
        assert!(e.is_empty());
    }

    #[test]
    fn email_rejects_local_part_edge_cases() {
        // Leading dot
        assert!(!is_email(".user@example.com"));
        // Trailing dot
        assert!(!is_email("user.@example.com"));
        // Consecutive dots
        assert!(!is_email("user..name@example.com"));
        // Valid with dots
        assert!(is_email("user.name@example.com"));
    }

    // -----------------------------------------------------------------------
    // Unit tests — URL validation
    // -----------------------------------------------------------------------

    #[test]
    fn url_valid() {
        assert!(is_url("https://example.com"));
        assert!(is_url("http://example.com"));
        assert!(is_url("https://example.com/path"));
        assert!(is_url("https://sub.example.com"));
        assert!(is_url("https://example.com?q=1"));
        assert!(is_url("https://example.com#frag"));
    }

    #[test]
    fn url_invalid() {
        assert!(!is_url(""));
        assert!(!is_url("ftp://example.com"));
        assert!(!is_url("example.com"));
        assert!(!is_url("https://"));
        assert!(!is_url("https://localhost"));
    }

    #[test]
    fn url_validator_on_errors() {
        let mut e = ValidationErrors::new();
        e.url("website", "not-a-url");
        assert!(!e.is_empty());

        let mut e = ValidationErrors::new();
        e.url("website", "https://example.com");
        assert!(e.is_empty());
    }

    // -----------------------------------------------------------------------
    // Unit tests — alphanumeric
    // -----------------------------------------------------------------------

    #[test]
    fn alphanumeric_valid() {
        let mut e = ValidationErrors::new();
        e.alphanumeric("code", "abc123");
        assert!(e.is_empty());
    }

    #[test]
    fn alphanumeric_invalid() {
        let mut e = ValidationErrors::new();
        e.alphanumeric("code", "abc-123");
        assert!(!e.is_empty());
    }

    // -----------------------------------------------------------------------
    // Unit tests — one_of
    // -----------------------------------------------------------------------

    #[test]
    fn one_of_valid() {
        let mut e = ValidationErrors::new();
        e.one_of("role", &"admin", &["admin", "user", "guest"]);
        assert!(e.is_empty());
    }

    #[test]
    fn one_of_invalid() {
        let mut e = ValidationErrors::new();
        e.one_of("role", &"superadmin", &["admin", "user", "guest"]);
        assert!(!e.is_empty());
        let msg = &e.field_errors("role").unwrap()[0].message;
        assert!(msg.contains("admin"));
        assert!(msg.contains("user"));
        assert!(msg.contains("guest"));
    }

    // -----------------------------------------------------------------------
    // Unit tests — contains_str
    // -----------------------------------------------------------------------

    #[test]
    fn contains_str_valid() {
        let mut e = ValidationErrors::new();
        e.contains_str("bio", "I love Rust", "Rust");
        assert!(e.is_empty());
    }

    #[test]
    fn contains_str_invalid() {
        let mut e = ValidationErrors::new();
        e.contains_str("bio", "I love Python", "Rust");
        assert!(!e.is_empty());
    }

    // -----------------------------------------------------------------------
    // Unit tests — custom and matches
    // -----------------------------------------------------------------------

    #[test]
    fn custom_validator() {
        let mut e = ValidationErrors::new();
        let password = "short";
        e.custom(
            "password",
            "weak",
            "password must contain a digit",
            password.chars().any(|c| c.is_ascii_digit()),
        );
        assert!(!e.is_empty());
        assert_eq!(e.field_errors("password").unwrap()[0].code, "weak");
    }

    #[test]
    fn custom_validator_passing() {
        let mut e = ValidationErrors::new();
        e.custom("field", "test", "fail", true);
        assert!(e.is_empty());
    }

    #[test]
    fn matches_validator() {
        let mut e = ValidationErrors::new();
        e.matches("slug", "hello world", |s| !s.contains(' '), "slug must not contain spaces");
        assert!(!e.is_empty());

        let mut e = ValidationErrors::new();
        e.matches("slug", "hello-world", |s| !s.contains(' '), "slug must not contain spaces");
        assert!(e.is_empty());
    }

    // -----------------------------------------------------------------------
    // Unit tests — Display and error response
    // -----------------------------------------------------------------------

    #[test]
    fn display_format() {
        let mut e = ValidationErrors::new();
        e.add("a", "x", "msg1");
        e.add("b", "y", "msg2");
        assert_eq!(format!("{e}"), "Validation failed: 2 error(s)");
    }

    #[tokio::test]
    async fn error_response_is_json() {
        let mut e = ValidationErrors::new();
        e.add("name", "required", "name is required");
        let resp = e.into_response();
        assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(
            resp.headers().get("content-type").unwrap(),
            "application/json"
        );
    }

    #[test]
    fn field_errors_returns_none_for_missing() {
        let e = ValidationErrors::new();
        assert!(e.field_errors("nonexistent").is_none());
    }
}
