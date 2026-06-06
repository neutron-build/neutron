//! GraphQL response — `{data, errors}` JSON envelope.

use neutron::handler::{Body, IntoResponse, Response};

/// A GraphQL error object in the response envelope.
#[derive(Debug, Clone, serde::Serialize)]
pub struct GraphQlError {
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<Vec<serde_json::Value>>,
}

impl GraphQlError {
    pub fn new(message: impl Into<String>) -> Self {
        Self { message: message.into(), path: None }
    }

    pub fn with_path(mut self, path: Vec<serde_json::Value>) -> Self {
        self.path = Some(path);
        self
    }
}

/// GraphQL response — always HTTP 200 per the spec.
///
/// Serializes to `{"data": ..., "errors": [...]}`.
/// If only errors are present, `data` is omitted. If only data, `errors` is omitted.
#[derive(Debug, Clone)]
pub struct GraphQlResponse {
    pub data:   Option<serde_json::Value>,
    pub errors: Vec<GraphQlError>,
}

impl GraphQlResponse {
    /// Successful response with data.
    pub fn ok(data: serde_json::Value) -> Self {
        Self { data: Some(data), errors: Vec::new() }
    }

    /// Error response (single error, no data).
    pub fn error(message: impl Into<String>) -> Self {
        Self { data: None, errors: vec![GraphQlError::new(message)] }
    }

    /// Multiple errors, no data.
    pub fn errors(errors: Vec<GraphQlError>) -> Self {
        Self { data: None, errors }
    }

    /// Partial response — data plus errors (common with field-level errors).
    pub fn partial(data: serde_json::Value, errors: Vec<GraphQlError>) -> Self {
        Self { data: Some(data), errors }
    }
}

impl IntoResponse for GraphQlResponse {
    fn into_response(self) -> Response {
        let mut obj = serde_json::Map::new();

        if let Some(data) = self.data {
            obj.insert("data".to_string(), data);
        }

        if !self.errors.is_empty() {
            obj.insert(
                "errors".to_string(),
                serde_json::to_value(&self.errors).unwrap_or(serde_json::Value::Array(vec![])),
            );
        }

        let body = serde_json::to_vec(&obj).unwrap_or_else(|_| b"{}".to_vec());

        http::Response::builder()
            .status(http::StatusCode::OK)
            .header("content-type", "application/json")
            .body(Body::full(body))
            .unwrap()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

    async fn body_str(resp: Response) -> String {
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn ok_response_has_data_field() {
        let resp = GraphQlResponse::ok(serde_json::json!({"name": "Alice"})).into_response();
        assert_eq!(resp.status(), http::StatusCode::OK);
        assert_eq!(resp.headers().get("content-type").unwrap(), "application/json");

        let body = body_str(resp).await;
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["data"]["name"], "Alice");
        assert!(v.get("errors").is_none());
    }

    #[tokio::test]
    async fn error_response_has_errors_field() {
        let resp = GraphQlResponse::error("field not found").into_response();
        let body = body_str(resp).await;
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["errors"][0]["message"], "field not found");
        assert!(v.get("data").is_none());
    }

    #[tokio::test]
    async fn partial_response_has_both_fields() {
        let resp = GraphQlResponse::partial(
            serde_json::json!({"user": null}),
            vec![GraphQlError::new("user not found")],
        )
        .into_response();
        let body = body_str(resp).await;
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert!(v.get("data").is_some());
        assert!(v.get("errors").is_some());
    }

    #[test]
    fn graphql_error_serializes_correctly() {
        let err = GraphQlError::new("something failed");
        let v = serde_json::to_value(&err).unwrap();
        assert_eq!(v["message"], "something failed");
        // path should not appear when None
        assert!(v.get("path").is_none());
    }
}
