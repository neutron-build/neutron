//! GraphQL request parsing from HTTP GET and POST.

use http::{Method, StatusCode};
use neutron::extract::FromRequest;
use neutron::handler::{IntoResponse, Request, Response};

/// A parsed GraphQL request extracted from HTTP GET or POST.
///
/// - **GET** — reads `query`, `variables` (JSON), and `operationName` from the query string.
/// - **POST `application/json`** — reads `{"query","variables","operationName"}` from the body.
/// - **POST `application/graphql`** — treats the entire body as the query string.
#[derive(Debug, Clone)]
pub struct GraphQlRequest {
    pub query:          String,
    pub variables:      Option<serde_json::Value>,
    pub operation_name: Option<String>,
}

impl FromRequest for GraphQlRequest {
    fn from_request(req: &Request) -> Result<Self, Response> {
        match *req.method() {
            Method::GET  => parse_from_query(req),
            Method::POST => parse_from_body(req),
            _ => Err((StatusCode::METHOD_NOT_ALLOWED, "GraphQL: use GET or POST").into_response()),
        }
    }
}

#[allow(clippy::result_large_err)]
fn parse_from_query(req: &Request) -> Result<GraphQlRequest, Response> {
    let qs = req.uri().query().unwrap_or("");

    let params: std::collections::HashMap<String, String> = serde_urlencoded::from_str(qs)
        .map_err(|_| (StatusCode::BAD_REQUEST, "Invalid query string").into_response())?;

    let query = params
        .get("query")
        .cloned()
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "Missing 'query' parameter").into_response())?;

    let variables      = params.get("variables").and_then(|v| serde_json::from_str(v).ok());
    let operation_name = params.get("operationName").cloned();

    Ok(GraphQlRequest { query, variables, operation_name })
}

#[allow(clippy::result_large_err)]
fn parse_from_body(req: &Request) -> Result<GraphQlRequest, Response> {
    let ct = req
        .headers()
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if ct.starts_with("application/json") {
        let v: serde_json::Value = serde_json::from_slice(req.body())
            .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid JSON: {e}")).into_response())?;

        let query = v["query"]
            .as_str()
            .ok_or_else(|| (StatusCode::BAD_REQUEST, "Missing 'query' field").into_response())?
            .to_string();

        let variables      = v.get("variables").filter(|v| !v.is_null()).cloned();
        let operation_name = v["operationName"].as_str().map(str::to_string);

        Ok(GraphQlRequest { query, variables, operation_name })
    } else if ct.starts_with("application/graphql") {
        let query = String::from_utf8(req.body().to_vec())
            .map_err(|_| (StatusCode::BAD_REQUEST, "Body must be UTF-8").into_response())?;
        Ok(GraphQlRequest { query, variables: None, operation_name: None })
    } else {
        Err((
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "GraphQL: expected application/json or application/graphql",
        )
            .into_response())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http::{HeaderMap, Method};
    use neutron::handler::Request;

    fn post_json(body: &str) -> Request {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/json".parse().unwrap());
        Request::new(
            Method::POST,
            "/graphql".parse().unwrap(),
            headers,
            Bytes::copy_from_slice(body.as_bytes()),
        )
    }

    fn get_query(qs: &str) -> Request {
        let uri = format!("/graphql?{qs}").parse().unwrap();
        Request::new(Method::GET, uri, HeaderMap::new(), Bytes::new())
    }

    fn ok_or_panic<T>(r: Result<T, Response>, msg: &str) -> T {
        match r { Ok(v) => v, Err(resp) => panic!("{msg}: HTTP {}", resp.status()) }
    }

    #[test]
    fn post_json_query_only() {
        let req = post_json(r#"{"query":"{ users { id } }"}"#);
        let gql = ok_or_panic(GraphQlRequest::from_request(&req), "parse failed");
        assert_eq!(gql.query, "{ users { id } }");
        assert!(gql.variables.is_none());
        assert!(gql.operation_name.is_none());
    }

    #[test]
    fn post_json_with_variables() {
        let req = post_json(r#"{"query":"query($id:ID!){user(id:$id){name}}","variables":{"id":"42"}}"#);
        let gql = ok_or_panic(GraphQlRequest::from_request(&req), "parse failed");
        assert_eq!(gql.variables.unwrap()["id"], "42");
    }

    #[test]
    fn post_json_with_operation_name() {
        let req = post_json(r#"{"query":"query GetUser { user { name } }","operationName":"GetUser"}"#);
        let gql = ok_or_panic(GraphQlRequest::from_request(&req), "parse failed");
        assert_eq!(gql.operation_name.unwrap(), "GetUser");
    }

    #[test]
    fn get_query_string() {
        let req = get_query("query=%7B+users+%7B+id+%7D+%7D");
        let gql = ok_or_panic(GraphQlRequest::from_request(&req), "parse failed");
        assert_eq!(gql.query, "{ users { id } }");
    }

    #[test]
    fn get_missing_query_param() {
        let req = get_query("operationName=Foo");
        let result = GraphQlRequest::from_request(&req);
        assert!(result.is_err());
        let resp = result.unwrap_err();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn post_wrong_content_type_rejected() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "text/plain".parse().unwrap());
        let req = Request::new(
            Method::POST, "/graphql".parse().unwrap(), headers, Bytes::from_static(b"{}"),
        );
        let result = GraphQlRequest::from_request(&req);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    #[test]
    fn application_graphql_content_type() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/graphql".parse().unwrap());
        let req = Request::new(
            Method::POST,
            "/graphql".parse().unwrap(),
            headers,
            Bytes::from_static(b"{ users { id } }"),
        );
        let gql = ok_or_panic(GraphQlRequest::from_request(&req), "parse failed");
        assert_eq!(gql.query, "{ users { id } }");
    }

    #[test]
    fn wrong_http_method() {
        let req = Request::new(
            Method::DELETE, "/graphql".parse().unwrap(), HeaderMap::new(), Bytes::new(),
        );
        let result = GraphQlRequest::from_request(&req);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status(), StatusCode::METHOD_NOT_ALLOWED);
    }
}
