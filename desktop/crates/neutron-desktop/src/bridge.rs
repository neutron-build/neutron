use http::Method;
use std::collections::HashMap;
use std::sync::Arc;

/// An incoming request from the `neutron://` protocol bridge.
#[derive(Debug)]
pub struct Request {
    pub method: Method,
    pub path: String,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
    pub query: HashMap<String, String>,
}

impl Request {
    /// Parse the request body as JSON.
    pub fn json<T: serde::de::DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_slice(&self.body)
    }

    /// Get a query parameter by name.
    pub fn query_param(&self, key: &str) -> Option<&str> {
        self.query.get(key).map(|s| s.as_str())
    }

    /// Get a header value by name (case-insensitive).
    pub fn header(&self, key: &str) -> Option<&str> {
        let lower = key.to_lowercase();
        self.headers
            .iter()
            .find(|(k, _)| k.to_lowercase() == lower)
            .map(|(_, v)| v.as_str())
    }
}

/// An outgoing response from the protocol bridge.
#[derive(Debug)]
pub struct Response {
    pub status: u16,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
}

impl Response {
    /// Create a 200 JSON response.
    pub fn json(value: &impl serde::Serialize) -> Self {
        let body = serde_json::to_vec(value).unwrap_or_default();
        let mut headers = HashMap::new();
        headers.insert("content-type".to_string(), "application/json".to_string());
        Self {
            status: 200,
            headers,
            body,
        }
    }

    /// Create a 200 response with a plain text body.
    pub fn text(body: impl Into<String>) -> Self {
        let body = body.into();
        let mut headers = HashMap::new();
        headers.insert("content-type".to_string(), "text/plain".to_string());
        Self {
            status: 200,
            headers,
            body: body.into_bytes(),
        }
    }

    /// Create an error response with RFC 7807 Problem Details.
    pub fn error(status: u16, title: &str, detail: &str) -> Self {
        let problem = serde_json::json!({
            "type": format!("about:blank"),
            "title": title,
            "status": status,
            "detail": detail,
        });
        let body = serde_json::to_vec(&problem).unwrap_or_default();
        let mut headers = HashMap::new();
        headers.insert(
            "content-type".to_string(),
            "application/problem+json".to_string(),
        );
        Self {
            status,
            headers,
            body,
        }
    }

    /// Create a 404 Not Found response.
    pub fn not_found() -> Self {
        Self::error(404, "Not Found", "The requested resource was not found")
    }

    /// Create a 204 No Content response.
    pub fn no_content() -> Self {
        Self {
            status: 204,
            headers: HashMap::new(),
            body: Vec::new(),
        }
    }
}

type HandlerFn = Box<dyn Fn(Request) -> Response + Send + Sync>;

struct RouteEntry {
    method: Method,
    path: String,
    handler: HandlerFn,
}

/// Router that matches incoming `neutron://` protocol requests to handlers.
pub struct Router {
    routes: Arc<Vec<RouteEntry>>,
}

impl Router {
    pub(crate) fn new(routes: Vec<super::Route>) -> Self {
        let entries = routes
            .into_iter()
            .map(|r| RouteEntry {
                method: r.method,
                path: r.path,
                handler: r.handler,
            })
            .collect();
        Self {
            routes: Arc::new(entries),
        }
    }

    /// Handle an incoming Tauri protocol request.
    pub fn handle(&self, tauri_request: tauri::http::Request<Vec<u8>>) -> tauri::http::Response<Vec<u8>> {
        let uri = tauri_request.uri().to_string();
        let parsed = url::Url::parse(&uri).unwrap_or_else(|_| {
            url::Url::parse("neutron://localhost/").expect("fallback URL")
        });

        let path = parsed.path().to_string();
        let method = tauri_request.method().clone();

        let query: HashMap<String, String> = parsed.query_pairs().into_owned().collect();

        let mut headers = HashMap::new();
        for (key, value) in tauri_request.headers() {
            if let Ok(v) = value.to_str() {
                headers.insert(key.to_string(), v.to_string());
            }
        }

        let request = Request {
            method: method.clone(),
            path: path.clone(),
            headers,
            body: tauri_request.body().clone(),
            query,
        };

        // Find matching route
        let response = self.dispatch(request);

        // Convert to Tauri response
        let mut builder = tauri::http::Response::builder().status(response.status);
        for (key, value) in &response.headers {
            builder = builder.header(key.as_str(), value.as_str());
        }
        builder.body(response.body).expect("response build failed")
    }

    /// Handle a pre-parsed [`Request`] directly (used by the dev TCP server).
    pub fn handle_direct(&self, request: Request) -> Response {
        self.dispatch(request)
    }

    /// Shared dispatch logic: match a request against registered routes.
    fn dispatch(&self, request: Request) -> Response {
        // Find the matching route index first, then move request into the handler.
        let matched = self
            .routes
            .iter()
            .position(|r| r.method == request.method && route_matches(&r.path, &request.path));

        match matched {
            Some(idx) => (self.routes[idx].handler)(request),
            None => Response::not_found(),
        }
    }
}

/// Simple path matching with `:param` support.
fn route_matches(pattern: &str, path: &str) -> bool {
    let pattern_parts: Vec<&str> = pattern.split('/').filter(|s| !s.is_empty()).collect();
    let path_parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

    if pattern_parts.len() != path_parts.len() {
        return false;
    }

    pattern_parts
        .iter()
        .zip(path_parts.iter())
        .all(|(p, a)| p.starts_with(':') || p == a)
}

/// Create the `neutron://` protocol handler for Tauri.
pub fn create_protocol_handler(
    router: Router,
) -> impl Fn(tauri::http::Request<Vec<u8>>) -> tauri::http::Response<Vec<u8>> + Send + Sync {
    move |request| router.handle(request)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    #[test]
    fn test_route_matching() {
        assert!(route_matches("/api/users", "/api/users"));
        assert!(route_matches("/api/users/:id", "/api/users/42"));
        assert!(!route_matches("/api/users", "/api/posts"));
        assert!(!route_matches("/api/users/:id", "/api/users/42/posts"));
    }

    #[test]
    fn test_response_json() {
        let resp = Response::json(&serde_json::json!({"ok": true}));
        assert_eq!(resp.status, 200);
        assert_eq!(
            resp.headers.get("content-type").unwrap(),
            "application/json"
        );
    }

    #[test]
    fn test_response_error() {
        let resp = Response::error(400, "Bad Request", "Invalid input");
        assert_eq!(resp.status, 400);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["status"], 400);
        assert_eq!(body["title"], "Bad Request");
    }
}
