//! RFC 7807 Problem Details error type.
//!
//! [`AppError`] implements the RFC 7807 Problem Details specification, providing
//! structured error responses with `application/problem+json` content type.
//!
//! All Neutron frameworks (Go, Python, Rust, TS, Zig) share this error format
//! per FRAMEWORK_CONTRACT.md.
//!
//! ```rust,ignore
//! async fn handler() -> Result<String, AppError> {
//!     Err(AppError::not_found("User 42 does not exist"))
//! }
//! ```
//!
//! Serialized format:
//! ```json
//! {
//!     "type": "https://neutron.dev/errors/not-found",
//!     "title": "Not Found",
//!     "status": 404,
//!     "detail": "User 42 does not exist"
//! }
//! ```

use http::StatusCode;
use std::fmt;

use crate::handler::{Body, IntoResponse, Response};

/// Base URL for Neutron error type URIs.
const ERROR_BASE_URL: &str = "https://neutron.dev/errors/";

/// A single field-level validation error.
#[derive(Debug, Clone)]
pub struct ValidationFieldError {
    /// The field that failed validation.
    pub field: String,
    /// Human-readable error message.
    pub message: String,
    /// The invalid value, if available.
    pub value: Option<String>,
}

impl ValidationFieldError {
    /// Create a new validation field error.
    pub fn new(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            message: message.into(),
            value: None,
        }
    }

    /// Create a validation field error with the rejected value.
    pub fn with_value(
        field: impl Into<String>,
        message: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        Self {
            field: field.into(),
            message: message.into(),
            value: Some(value.into()),
        }
    }
}

/// Application error implementing RFC 7807 Problem Details.
///
/// Carries an HTTP status code, a machine-readable error type URI,
/// a human-readable title, a specific detail message, and optional
/// field-level validation errors.
#[derive(Debug, Clone)]
pub struct AppError {
    /// HTTP status code.
    pub status: StatusCode,
    /// Machine-readable error type URI (e.g. `https://neutron.dev/errors/not-found`).
    pub error_type: String,
    /// Short human-readable summary (e.g. "Not Found").
    pub title: String,
    /// Specific error detail for this occurrence.
    pub detail: String,
    /// Optional request path that triggered the error.
    pub instance: Option<String>,
    /// Field-level validation errors (only for 422 responses).
    pub errors: Vec<ValidationFieldError>,
}

impl AppError {
    /// Create a new error with full control over all fields.
    pub fn new(
        status: StatusCode,
        code: impl Into<String>,
        title: impl Into<String>,
        detail: impl Into<String>,
    ) -> Self {
        let code = code.into();
        let error_type = if code.starts_with("https://") {
            code
        } else {
            format!("{}{}", ERROR_BASE_URL, code)
        };
        Self {
            status,
            error_type,
            title: title.into(),
            detail: detail.into(),
            instance: None,
            errors: Vec::new(),
        }
    }

    /// Set the `instance` field (request path).
    pub fn with_instance(mut self, instance: impl Into<String>) -> Self {
        self.instance = Some(instance.into());
        self
    }

    /// Attach field-level validation errors.
    pub fn with_errors(mut self, errors: Vec<ValidationFieldError>) -> Self {
        self.errors = errors;
        self
    }

    // --- Convenience constructors (per FRAMEWORK_CONTRACT.md) ---

    /// 400 Bad Request
    pub fn bad_request(detail: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "bad-request", "Bad Request", detail)
    }

    /// 401 Unauthorized
    pub fn unauthorized(detail: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "unauthorized", "Unauthorized", detail)
    }

    /// 403 Forbidden
    pub fn forbidden(detail: impl Into<String>) -> Self {
        Self::new(StatusCode::FORBIDDEN, "forbidden", "Forbidden", detail)
    }

    /// 404 Not Found
    pub fn not_found(detail: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "not-found", "Not Found", detail)
    }

    /// 409 Conflict
    pub fn conflict(detail: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, "conflict", "Conflict", detail)
    }

    /// 422 Validation Failed — with field-level errors.
    pub fn validation_error(
        detail: impl Into<String>,
        errors: Vec<ValidationFieldError>,
    ) -> Self {
        Self::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "validation",
            "Validation Failed",
            detail,
        )
        .with_errors(errors)
    }

    /// 429 Rate Limited
    pub fn rate_limited(detail: impl Into<String>) -> Self {
        Self::new(
            StatusCode::TOO_MANY_REQUESTS,
            "rate-limited",
            "Rate Limited",
            detail,
        )
    }

    /// 500 Internal Server Error
    pub fn internal(detail: impl Into<String>) -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            "Internal Server Error",
            detail,
        )
    }
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.title, self.detail)
    }
}

impl std::error::Error for AppError {}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        // With JSON: serialize to RFC 7807 Problem Details
        #[cfg(feature = "json")]
        {
            let mut problem = serde_json::json!({
                "type": self.error_type,
                "title": self.title,
                "status": self.status.as_u16(),
                "detail": self.detail,
            });

            if let Some(ref instance) = self.instance {
                problem["instance"] = serde_json::Value::String(instance.clone());
            }

            if !self.errors.is_empty() {
                let errors: Vec<serde_json::Value> = self
                    .errors
                    .iter()
                    .map(|e| {
                        let mut obj = serde_json::json!({
                            "field": e.field,
                            "message": e.message,
                        });
                        if let Some(ref val) = e.value {
                            obj["value"] = serde_json::Value::String(val.clone());
                        }
                        obj
                    })
                    .collect();
                problem["errors"] = serde_json::Value::Array(errors);
            }

            let body_bytes = serde_json::to_vec(&problem).unwrap_or_default();
            http::Response::builder()
                .status(self.status)
                .header("content-type", "application/problem+json")
                .body(Body::full(body_bytes))
                .unwrap()
        }
        // Without JSON: plain text fallback
        #[cfg(not(feature = "json"))]
        {
            http::Response::builder()
                .status(self.status)
                .header("content-type", "text/plain; charset=utf-8")
                .body(Body::full(format!(
                    "{} {}: {}",
                    self.status.as_u16(),
                    self.title,
                    self.detail
                )))
                .unwrap()
        }
    }
}

// ---------------------------------------------------------------------------
// From impls for ergonomic error conversion
// ---------------------------------------------------------------------------

impl From<std::io::Error> for AppError {
    fn from(e: std::io::Error) -> Self {
        AppError::internal(e.to_string())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bad_request_fields() {
        let err = AppError::bad_request("missing field 'name'");
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert_eq!(err.error_type, "https://neutron.dev/errors/bad-request");
        assert_eq!(err.title, "Bad Request");
        assert_eq!(err.detail, "missing field 'name'");
    }

    #[test]
    fn not_found_fields() {
        let err = AppError::not_found("User 42 does not exist");
        assert_eq!(err.status, StatusCode::NOT_FOUND);
        assert_eq!(err.error_type, "https://neutron.dev/errors/not-found");
        assert_eq!(err.title, "Not Found");
    }

    #[test]
    fn unauthorized_fields() {
        let err = AppError::unauthorized("invalid token");
        assert_eq!(err.status, StatusCode::UNAUTHORIZED);
        assert_eq!(err.error_type, "https://neutron.dev/errors/unauthorized");
    }

    #[test]
    fn forbidden_fields() {
        let err = AppError::forbidden("insufficient permissions");
        assert_eq!(err.status, StatusCode::FORBIDDEN);
        assert_eq!(err.error_type, "https://neutron.dev/errors/forbidden");
    }

    #[test]
    fn conflict_fields() {
        let err = AppError::conflict("email already exists");
        assert_eq!(err.status, StatusCode::CONFLICT);
        assert_eq!(err.error_type, "https://neutron.dev/errors/conflict");
    }

    #[test]
    fn rate_limited_fields() {
        let err = AppError::rate_limited("too many requests");
        assert_eq!(err.status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(err.error_type, "https://neutron.dev/errors/rate-limited");
    }

    #[test]
    fn internal_fields() {
        let err = AppError::internal("unexpected failure");
        assert_eq!(err.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(err.error_type, "https://neutron.dev/errors/internal");
    }

    #[test]
    fn validation_error_with_fields() {
        let err = AppError::validation_error(
            "Request body failed validation",
            vec![
                ValidationFieldError::with_value("email", "must be a valid email", "not-an-email"),
                ValidationFieldError::new("name", "is required"),
            ],
        );
        assert_eq!(err.status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(err.error_type, "https://neutron.dev/errors/validation");
        assert_eq!(err.errors.len(), 2);
        assert_eq!(err.errors[0].field, "email");
        assert_eq!(err.errors[0].value.as_deref(), Some("not-an-email"));
        assert_eq!(err.errors[1].field, "name");
        assert!(err.errors[1].value.is_none());
    }

    #[test]
    fn with_instance() {
        let err = AppError::not_found("User 42 does not exist")
            .with_instance("/api/users/42");
        assert_eq!(err.instance.as_deref(), Some("/api/users/42"));
    }

    #[test]
    fn display_format() {
        let err = AppError::not_found("User 42 does not exist");
        assert_eq!(format!("{err}"), "Not Found: User 42 does not exist");
    }

    #[test]
    fn custom_error_type_url() {
        let err = AppError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "https://neutron.dev/errors/nucleus-required",
            "Nucleus Required",
            "This feature requires Nucleus database",
        );
        assert_eq!(
            err.error_type,
            "https://neutron.dev/errors/nucleus-required"
        );
    }

    #[test]
    fn code_without_url_prefix() {
        let err = AppError::new(StatusCode::BAD_REQUEST, "custom-error", "Custom", "detail");
        assert_eq!(
            err.error_type,
            "https://neutron.dev/errors/custom-error"
        );
    }
}
