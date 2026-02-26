//! Automatic OpenAPI 3.1 specification generation.
//!
//! Build an OpenAPI spec declaratively and serve Swagger UI alongside your API.
//!
//! # Example
//!
//! ```rust,ignore
//! use neutron::prelude::*;
//! use neutron::openapi::{OpenApi, ApiRoute, Schema};
//!
//! let spec = OpenApi::new("My API", "1.0.0")
//!     .description("A sample API")
//!     .route(
//!         ApiRoute::get("/users")
//!             .summary("List all users")
//!             .tag("users")
//!             .response(200, "application/json", Schema::array(Schema::ref_to("User")))
//!     )
//!     .schema("User", Schema::object()
//!         .property("id", Schema::integer())
//!         .property("name", Schema::string())
//!     );
//!
//! let router = Router::new()
//!     .get("/docs", spec.swagger_ui())
//!     .get("/openapi.json", spec.json_handler())
//!     .get("/users", list_users);
//! ```

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use http::StatusCode;
use serde_json::{json, Value};

use crate::handler::{Body, Response};

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

/// JSON Schema builder for OpenAPI type definitions.
#[derive(Debug, Clone)]
pub struct Schema(pub Value);

impl Schema {
    /// String type.
    pub fn string() -> Self {
        Self(json!({"type": "string"}))
    }

    /// String with format (e.g. `"email"`, `"uri"`, `"date-time"`).
    pub fn string_format(format: &str) -> Self {
        Self(json!({"type": "string", "format": format}))
    }

    /// Integer type.
    pub fn integer() -> Self {
        Self(json!({"type": "integer"}))
    }

    /// 64-bit integer.
    pub fn int64() -> Self {
        Self(json!({"type": "integer", "format": "int64"}))
    }

    /// Number (floating point).
    pub fn number() -> Self {
        Self(json!({"type": "number"}))
    }

    /// Boolean.
    pub fn boolean() -> Self {
        Self(json!({"type": "boolean"}))
    }

    /// Array of items.
    pub fn array(items: Schema) -> Self {
        Self(json!({"type": "array", "items": items.0}))
    }

    /// Object with properties.
    pub fn object() -> ObjectSchema {
        ObjectSchema {
            properties: HashMap::new(),
            required: Vec::new(),
            description: None,
        }
    }

    /// Reference to a named schema (`$ref`).
    pub fn ref_to(name: &str) -> Self {
        Self(json!({"$ref": format!("#/components/schemas/{name}")}))
    }

    /// Enum of allowed values.
    pub fn enumeration(values: &[&str]) -> Self {
        Self(json!({"type": "string", "enum": values}))
    }

    /// Nullable wrapper.
    pub fn nullable(inner: Schema) -> Self {
        let mut v = inner.0;
        if let Some(obj) = v.as_object_mut() {
            obj.insert("nullable".to_string(), json!(true));
        }
        Self(v)
    }

    /// oneOf combinator.
    pub fn one_of(schemas: Vec<Schema>) -> Self {
        let items: Vec<Value> = schemas.into_iter().map(|s| s.0).collect();
        Self(json!({"oneOf": items}))
    }

    /// Set description.
    pub fn description(mut self, desc: &str) -> Self {
        if let Some(obj) = self.0.as_object_mut() {
            obj.insert("description".to_string(), json!(desc));
        }
        self
    }

    /// Set example value.
    pub fn example(mut self, example: Value) -> Self {
        if let Some(obj) = self.0.as_object_mut() {
            obj.insert("example".to_string(), example);
        }
        self
    }

    /// Raw JSON Schema value.
    pub fn raw(value: Value) -> Self {
        Self(value)
    }
}

/// Builder for object schemas.
pub struct ObjectSchema {
    properties: HashMap<String, Value>,
    required: Vec<String>,
    description: Option<String>,
}

impl ObjectSchema {
    /// Add a property.
    pub fn property(mut self, name: &str, schema: Schema) -> Self {
        self.properties.insert(name.to_string(), schema.0);
        self
    }

    /// Mark fields as required.
    pub fn required(mut self, fields: &[&str]) -> Self {
        self.required.extend(fields.iter().map(|s| s.to_string()));
        self
    }

    /// Set description.
    pub fn description(mut self, desc: &str) -> Self {
        self.description = Some(desc.to_string());
        self
    }

    /// Build into a Schema.
    pub fn build(self) -> Schema {
        let mut obj = json!({
            "type": "object",
            "properties": self.properties,
        });
        if !self.required.is_empty() {
            obj["required"] = json!(self.required);
        }
        if let Some(desc) = self.description {
            obj["description"] = json!(desc);
        }
        Schema(obj)
    }
}

impl From<ObjectSchema> for Schema {
    fn from(obj: ObjectSchema) -> Self {
        obj.build()
    }
}

// ---------------------------------------------------------------------------
// Parameter
// ---------------------------------------------------------------------------

/// OpenAPI parameter (query, path, header).
#[derive(Debug, Clone)]
pub struct Parameter {
    name: String,
    location: &'static str,
    required: bool,
    schema: Schema,
    description: Option<String>,
}

impl Parameter {
    /// Path parameter (required by default).
    pub fn path(name: &str, schema: Schema) -> Self {
        Self {
            name: name.to_string(),
            location: "path",
            required: true,
            schema,
            description: None,
        }
    }

    /// Query parameter (optional by default).
    pub fn query(name: &str, schema: Schema) -> Self {
        Self {
            name: name.to_string(),
            location: "query",
            required: false,
            schema,
            description: None,
        }
    }

    /// Header parameter.
    pub fn header(name: &str, schema: Schema) -> Self {
        Self {
            name: name.to_string(),
            location: "header",
            required: false,
            schema,
            description: None,
        }
    }

    /// Mark as required.
    pub fn required(mut self) -> Self {
        self.required = true;
        self
    }

    /// Set description.
    pub fn description(mut self, desc: &str) -> Self {
        self.description = Some(desc.to_string());
        self
    }

    fn to_value(&self) -> Value {
        let mut v = json!({
            "name": self.name,
            "in": self.location,
            "required": self.required,
            "schema": self.schema.0,
        });
        if let Some(ref desc) = self.description {
            v["description"] = json!(desc);
        }
        v
    }
}

// ---------------------------------------------------------------------------
// ApiRoute
// ---------------------------------------------------------------------------

/// Describes a single API route for the OpenAPI spec.
#[derive(Debug, Clone)]
pub struct ApiRoute {
    method: String,
    path: String,
    summary: Option<String>,
    description: Option<String>,
    tags: Vec<String>,
    parameters: Vec<Parameter>,
    request_body: Option<(String, Schema)>,
    responses: Vec<(u16, String, Option<String>, Option<Schema>)>,
    deprecated: bool,
    operation_id: Option<String>,
}

impl ApiRoute {
    fn new(method: &str, path: &str) -> Self {
        Self {
            method: method.to_lowercase(),
            path: path.to_string(),
            summary: None,
            description: None,
            tags: Vec::new(),
            parameters: Vec::new(),
            request_body: None,
            responses: Vec::new(),
            deprecated: false,
            operation_id: None,
        }
    }

    pub fn get(path: &str) -> Self { Self::new("get", path) }
    pub fn post(path: &str) -> Self { Self::new("post", path) }
    pub fn put(path: &str) -> Self { Self::new("put", path) }
    pub fn delete(path: &str) -> Self { Self::new("delete", path) }
    pub fn patch(path: &str) -> Self { Self::new("patch", path) }

    // -- Internal helpers used by Router::openapi() -------------------------

    /// Return the HTTP method (lowercase).
    pub(crate) fn method(&self) -> &str { &self.method }

    /// Return the path.
    pub(crate) fn path(&self) -> &str { &self.path }

    /// Create a minimal stub ApiRoute for any HTTP method string (lowercase).
    pub(crate) fn for_method(method: &str, path: &str) -> Self {
        Self::new(method, path)
    }

    /// Clone this route with `prefix` prepended to the path.
    pub(crate) fn with_prefix(&self, prefix: &str) -> Self {
        let mut cloned = self.clone();
        cloned.path = format!("{prefix}{}", cloned.path);
        cloned
    }

    /// Short summary.
    pub fn summary(mut self, summary: &str) -> Self {
        self.summary = Some(summary.to_string());
        self
    }

    /// Detailed description.
    pub fn description(mut self, desc: &str) -> Self {
        self.description = Some(desc.to_string());
        self
    }

    /// Add a tag.
    pub fn tag(mut self, tag: &str) -> Self {
        self.tags.push(tag.to_string());
        self
    }

    /// Set operation ID.
    pub fn operation_id(mut self, id: &str) -> Self {
        self.operation_id = Some(id.to_string());
        self
    }

    /// Mark as deprecated.
    pub fn deprecated(mut self) -> Self {
        self.deprecated = true;
        self
    }

    /// Add a parameter.
    pub fn param(mut self, param: Parameter) -> Self {
        self.parameters.push(param);
        self
    }

    /// Set the request body.
    pub fn body(mut self, content_type: &str, schema: Schema) -> Self {
        self.request_body = Some((content_type.to_string(), schema));
        self
    }

    /// Add a response.
    pub fn response(mut self, status: u16, content_type: &str, schema: Schema) -> Self {
        self.responses
            .push((status, content_type.to_string(), None, Some(schema)));
        self
    }

    /// Add a response with description.
    pub fn response_desc(mut self, status: u16, desc: &str) -> Self {
        self.responses
            .push((status, String::new(), Some(desc.to_string()), None));
        self
    }

    fn to_operation(&self) -> Value {
        let mut op = json!({});

        if let Some(ref s) = self.summary {
            op["summary"] = json!(s);
        }
        if let Some(ref d) = self.description {
            op["description"] = json!(d);
        }
        if !self.tags.is_empty() {
            op["tags"] = json!(self.tags);
        }
        if let Some(ref id) = self.operation_id {
            op["operationId"] = json!(id);
        }
        if self.deprecated {
            op["deprecated"] = json!(true);
        }

        if !self.parameters.is_empty() {
            op["parameters"] = json!(
                self.parameters.iter().map(|p| p.to_value()).collect::<Vec<_>>()
            );
        }

        if let Some((ref ct, ref schema)) = self.request_body {
            op["requestBody"] = json!({
                "required": true,
                "content": {
                    ct: { "schema": schema.0 }
                }
            });
        }

        if !self.responses.is_empty() {
            let mut responses = json!({});
            for (status, ct, desc, schema) in &self.responses {
                let key = status.to_string();
                let mut resp = json!({});
                if let Some(d) = desc {
                    resp["description"] = json!(d);
                } else {
                    resp["description"] = json!("");
                }
                if let Some(s) = schema {
                    resp["content"] = json!({
                        ct: { "schema": s.0 }
                    });
                }
                responses[key] = resp;
            }
            op["responses"] = responses;
        } else {
            op["responses"] = json!({"200": {"description": "OK"}});
        }

        op
    }
}

// ---------------------------------------------------------------------------
// OpenApi
// ---------------------------------------------------------------------------

/// OpenAPI 3.1 specification builder.
pub struct OpenApi {
    title: String,
    version: String,
    description: Option<String>,
    routes: Vec<ApiRoute>,
    schemas: HashMap<String, Schema>,
    servers: Vec<(String, Option<String>)>,
    tags: Vec<(String, Option<String>)>,
}

impl OpenApi {
    /// Create a new OpenAPI spec with title and version.
    pub fn new(title: &str, version: &str) -> Self {
        Self {
            title: title.to_string(),
            version: version.to_string(),
            description: None,
            routes: Vec::new(),
            schemas: HashMap::new(),
            servers: Vec::new(),
            tags: Vec::new(),
        }
    }

    /// Set description.
    pub fn description(mut self, desc: &str) -> Self {
        self.description = Some(desc.to_string());
        self
    }

    /// Add a server URL.
    pub fn server(mut self, url: &str, description: Option<&str>) -> Self {
        self.servers
            .push((url.to_string(), description.map(|s| s.to_string())));
        self
    }

    /// Add a tag with optional description.
    pub fn tag(mut self, name: &str, description: Option<&str>) -> Self {
        self.tags
            .push((name.to_string(), description.map(|s| s.to_string())));
        self
    }

    /// Add a route to the spec.
    pub fn route(mut self, route: ApiRoute) -> Self {
        self.routes.push(route);
        self
    }

    /// Add a named schema to components.
    pub fn schema(mut self, name: &str, schema: impl Into<Schema>) -> Self {
        self.schemas.insert(name.to_string(), schema.into());
        self
    }

    /// Render the OpenAPI spec as a JSON Value.
    pub fn to_json(&self) -> Value {
        let mut spec = json!({
            "openapi": "3.1.0",
            "info": {
                "title": self.title,
                "version": self.version,
            }
        });

        if let Some(ref desc) = self.description {
            spec["info"]["description"] = json!(desc);
        }

        if !self.servers.is_empty() {
            let servers: Vec<Value> = self
                .servers
                .iter()
                .map(|(url, desc)| {
                    let mut s = json!({"url": url});
                    if let Some(d) = desc {
                        s["description"] = json!(d);
                    }
                    s
                })
                .collect();
            spec["servers"] = json!(servers);
        }

        if !self.tags.is_empty() {
            let tags: Vec<Value> = self
                .tags
                .iter()
                .map(|(name, desc)| {
                    let mut t = json!({"name": name});
                    if let Some(d) = desc {
                        t["description"] = json!(d);
                    }
                    t
                })
                .collect();
            spec["tags"] = json!(tags);
        }

        // Build paths
        let mut paths: HashMap<String, Value> = HashMap::new();
        for route in &self.routes {
            let operation = route.to_operation();
            let path_item = paths.entry(route.path.clone()).or_insert_with(|| json!({}));
            path_item[&route.method] = operation;
        }
        spec["paths"] = json!(paths);

        // Build components/schemas
        if !self.schemas.is_empty() {
            let schemas: HashMap<String, Value> = self
                .schemas
                .iter()
                .map(|(name, schema)| (name.clone(), schema.0.clone()))
                .collect();
            spec["components"] = json!({"schemas": schemas});
        }

        spec
    }

    /// Render the OpenAPI spec as a JSON string.
    pub fn to_json_string(&self) -> String {
        serde_json::to_string_pretty(&self.to_json()).unwrap()
    }

    /// Get a handler that serves the OpenAPI JSON spec.
    pub fn json_handler(
        &self,
    ) -> impl Fn() -> Pin<Box<dyn Future<Output = Response> + Send>>
           + Clone
           + Send
           + Sync
           + 'static {
        let json = Arc::new(self.to_json_string());
        move || {
            let json = Arc::clone(&json);
            Box::pin(async move {
                http::Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "application/json")
                    .body(Body::full(json.as_bytes().to_vec()))
                    .unwrap()
            })
        }
    }

    /// Get a handler that serves the Swagger UI HTML page.
    ///
    /// The UI loads the spec from `spec_url` (default: `"/openapi.json"`).
    pub fn swagger_ui(&self) -> impl Fn() -> Pin<Box<dyn Future<Output = Response> + Send>>
           + Clone
           + Send
           + Sync
           + 'static {
        self.swagger_ui_at("/openapi.json")
    }

    /// Generate a TypeScript fetch client from this OpenAPI spec.
    ///
    /// Emits:
    /// - `interface` declarations for every named schema in `components/schemas`
    /// - One `async function` per route with typed path/query params and return type
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let ts = spec.generate_typescript("https://api.example.com");
    /// std::fs::write("client.ts", ts).unwrap();
    /// ```
    pub fn generate_typescript(&self, base_url: &str) -> String {
        let mut out = String::new();

        // ---- Header --------------------------------------------------------
        out.push_str("// AUTO-GENERATED by neutron OpenAPI codegen. DO NOT EDIT.\n\n");
        out.push_str(&format!("const BASE_URL = \"{base_url}\";\n\n"));

        // ---- Interfaces for components/schemas -----------------------------
        if !self.schemas.is_empty() {
            out.push_str("// ── Schemas ──────────────────────────────────────────────────────\n\n");
            // Sort for deterministic output
            let mut schema_names: Vec<&String> = self.schemas.keys().collect();
            schema_names.sort();
            for name in schema_names {
                let schema = &self.schemas[name];
                out.push_str(&schema_to_ts_interface(name, &schema.0));
                out.push('\n');
            }
        }

        // ---- One function per route ----------------------------------------
        out.push_str("// ── API Functions ────────────────────────────────────────────────\n\n");
        for route in &self.routes {
            out.push_str(&route_to_ts_function(route));
            out.push('\n');
        }

        out
    }

    /// Get a handler that serves the Swagger UI, loading the spec from a custom URL.
    pub fn swagger_ui_at(
        &self,
        spec_url: &str,
    ) -> impl Fn() -> Pin<Box<dyn Future<Output = Response> + Send>>
           + Clone
           + Send
           + Sync
           + 'static {
        let html = Arc::new(swagger_html(spec_url));
        move || {
            let html = Arc::clone(&html);
            Box::pin(async move {
                http::Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "text/html; charset=utf-8")
                    .body(Body::full(html.as_bytes().to_vec()))
                    .unwrap()
            })
        }
    }
}

// ---------------------------------------------------------------------------
// TypeScript codegen helpers
// ---------------------------------------------------------------------------

/// Convert a JSON Schema value to a TypeScript type annotation.
fn schema_to_ts_type(schema: &Value) -> String {
    if let Some(ref_path) = schema.get("$ref").and_then(|v| v.as_str()) {
        // "#/components/schemas/Foo" → "Foo"
        return ref_path.split('/').last().unwrap_or("unknown").to_string();
    }
    let ty = schema.get("type").and_then(|v| v.as_str()).unwrap_or("");
    match ty {
        "string"  => "string".to_string(),
        "integer" | "number" => "number".to_string(),
        "boolean" => "boolean".to_string(),
        "array"   => {
            let item_type = schema
                .get("items")
                .map(|items| schema_to_ts_type(items))
                .unwrap_or_else(|| "unknown".to_string());
            format!("{item_type}[]")
        }
        "object" => {
            // Inline object → Record or anonymous interface
            if let Some(props) = schema.get("properties").and_then(|p| p.as_object()) {
                let required: Vec<&str> = schema
                    .get("required")
                    .and_then(|r| r.as_array())
                    .map(|r| r.iter().filter_map(|v| v.as_str()).collect())
                    .unwrap_or_default();
                let fields: Vec<String> = props
                    .iter()
                    .map(|(k, v)| {
                        let opt = if required.contains(&k.as_str()) { "" } else { "?" };
                        format!("  {k}{opt}: {}", schema_to_ts_type(v))
                    })
                    .collect();
                format!("{{\n{}\n}}", fields.join(";\n"))
            } else {
                "Record<string, unknown>".to_string()
            }
        }
        _ => {
            // oneOf / anyOf
            if let Some(one_of) = schema.get("oneOf").and_then(|v| v.as_array()) {
                let types: Vec<String> = one_of.iter().map(schema_to_ts_type).collect();
                return types.join(" | ");
            }
            "unknown".to_string()
        }
    }
}

/// Generate a TypeScript `interface` declaration for a named schema.
fn schema_to_ts_interface(name: &str, schema: &Value) -> String {
    let ty = schema.get("type").and_then(|v| v.as_str()).unwrap_or("");
    if ty == "object" {
        let props = schema.get("properties").and_then(|p| p.as_object());
        let required: Vec<&str> = schema
            .get("required")
            .and_then(|r| r.as_array())
            .map(|r| r.iter().filter_map(|v| v.as_str()).collect())
            .unwrap_or_default();
        let mut out = format!("export interface {name} {{\n");
        if let Some(props) = props {
            let mut keys: Vec<&String> = props.keys().collect();
            keys.sort();
            for key in keys {
                let v = &props[key];
                let opt = if required.contains(&key.as_str()) { "" } else { "?" };
                out.push_str(&format!("  {key}{opt}: {};\n", schema_to_ts_type(v)));
            }
        }
        out.push('}');
        out
    } else {
        // Type alias for non-object schemas
        format!("export type {name} = {};", schema_to_ts_type(schema))
    }
}

/// Convert a neutron-style path `/users/:id/posts` to a TypeScript template literal
/// `\`${BASE_URL}/users/${id}/posts\``.
fn path_to_ts_template(path: &str) -> String {
    let converted = path.split('/').map(|segment| {
        if let Some(name) = segment.strip_prefix(':') {
            format!("${{{name}}}")
        } else {
            segment.to_string()
        }
    }).collect::<Vec<_>>().join("/");
    format!("`${{BASE_URL}}{converted}`")
}

/// Produce a camelCase function name from a method + path.
/// `get /users/:id` → `getUsersById`
fn route_to_function_name(method: &str, path: &str) -> String {
    let mut parts = vec![method.to_lowercase()];
    for segment in path.split('/') {
        if segment.is_empty() { continue; }
        if let Some(name) = segment.strip_prefix(':') {
            parts.push(format!("By{}", capitalise(name)));
        } else {
            parts.push(capitalise(segment));
        }
    }
    parts.join("")
}

fn capitalise(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        None => String::new(),
        Some(f) => f.to_uppercase().to_string() + c.as_str(),
    }
}

/// Generate a TypeScript `async function` for a single API route.
fn route_to_ts_function(route: &ApiRoute) -> String {
    let fn_name = route.operation_id.clone()
        .unwrap_or_else(|| route_to_function_name(&route.method, &route.path));

    // Collect path and query parameters
    let path_params: Vec<&Parameter> = route.parameters.iter()
        .filter(|p| p.location == "path").collect();
    let query_params: Vec<&Parameter> = route.parameters.iter()
        .filter(|p| p.location == "query").collect();

    // Return type — use first 2xx response schema if available
    let return_type = route.responses.iter()
        .find(|(status, _, _, _)| *status >= 200 && *status < 300)
        .and_then(|(_, _, _, schema)| schema.as_ref())
        .map(|s| schema_to_ts_type(&s.0))
        .unwrap_or_else(|| "void".to_string());

    // Build function params signature
    let mut params: Vec<String> = Vec::new();
    for p in &path_params {
        let ts_type = schema_to_ts_type(&p.schema.0);
        params.push(format!("{}: {}", p.name, ts_type));
    }
    if !query_params.is_empty() {
        let fields: Vec<String> = query_params.iter().map(|p| {
            let opt = if p.required { "" } else { "?" };
            format!("{}{}?: {}", p.name, opt, schema_to_ts_type(&p.schema.0))
        }).collect();
        params.push(format!("query?: {{ {} }}", fields.join(", ")));
    }
    if let Some((ct, schema)) = &route.request_body {
        let ts_type = schema_to_ts_type(&schema.0);
        let param_name = if ct.contains("json") { "body" } else { "body" };
        params.push(format!("{}: {}", param_name, ts_type));
    }

    let params_str = params.join(", ");
    let url = path_to_ts_template(&route.path);
    let method_upper = route.method.to_uppercase();

    // Build fetch body
    let has_body = route.request_body.is_some();
    let fetch_opts = if has_body {
        format!(
            r#"{{
    method: "{method_upper}",
    headers: {{ "Content-Type": "application/json" }},
    body: JSON.stringify(body),
  }}"#
        )
    } else {
        format!("{{ method: \"{method_upper}\" }}")
    };

    // Build query string
    let query_suffix = if !query_params.is_empty() {
        r#"
  const qs = query ? "?" + new URLSearchParams(query as Record<string, string>).toString() : "";"#
            .to_string()
    } else {
        String::new()
    };
    let url_with_qs = if !query_params.is_empty() {
        format!("{url} + qs")
    } else {
        url
    };

    // Function signature with optional JSDoc summary
    let mut out = String::new();
    if let Some(ref summary) = route.summary {
        out.push_str(&format!("/** {summary} */\n"));
    }
    out.push_str(&format!(
        "export async function {fn_name}({params_str}): Promise<{return_type}> {{\n"
    ));
    if !query_params.is_empty() {
        out.push_str(&query_suffix);
        out.push('\n');
    }
    out.push_str(&format!(
        "  const res = await fetch({url_with_qs}, {fetch_opts});\n"
    ));
    if return_type != "void" {
        out.push_str("  return res.json();\n");
    }
    out.push('}');
    out
}

fn swagger_html(spec_url: &str) -> String {
    format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>API Docs</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css"/>
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>
    SwaggerUIBundle({{
      url: "{spec_url}",
      dom_id: '#swagger-ui',
      presets: [SwaggerUIBundle.presets.apis, SwaggerUIBundle.SwaggerUIStandalonePreset],
      layout: "StandaloneLayout"
    }});
  </script>
</body>
</html>"#
    )
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::router::Router;
    use crate::testing::TestClient;

    fn sample_spec() -> OpenApi {
        OpenApi::new("Test API", "1.0.0")
            .description("A test API")
            .server("http://localhost:3000", Some("Local"))
            .tag("users", Some("User operations"))
            .route(
                ApiRoute::get("/users")
                    .summary("List users")
                    .tag("users")
                    .param(Parameter::query("page", Schema::integer()).description("Page number"))
                    .response(
                        200,
                        "application/json",
                        Schema::array(Schema::ref_to("User")),
                    ),
            )
            .route(
                ApiRoute::post("/users")
                    .summary("Create user")
                    .tag("users")
                    .body(
                        "application/json",
                        Schema::ref_to("CreateUser"),
                    )
                    .response(201, "application/json", Schema::ref_to("User")),
            )
            .route(
                ApiRoute::get("/users/:id")
                    .summary("Get user by ID")
                    .tag("users")
                    .param(Parameter::path("id", Schema::integer()))
                    .response(200, "application/json", Schema::ref_to("User"))
                    .response_desc(404, "User not found"),
            )
            .route(
                ApiRoute::delete("/users/:id")
                    .summary("Delete user")
                    .tag("users")
                    .deprecated(),
            )
            .schema(
                "User",
                Schema::object()
                    .property("id", Schema::integer())
                    .property("name", Schema::string())
                    .property("email", Schema::string_format("email"))
                    .required(&["id", "name", "email"]),
            )
            .schema(
                "CreateUser",
                Schema::object()
                    .property("name", Schema::string())
                    .property("email", Schema::string_format("email"))
                    .required(&["name", "email"]),
            )
    }

    #[test]
    fn spec_has_openapi_version() {
        let spec = sample_spec().to_json();
        assert_eq!(spec["openapi"], "3.1.0");
    }

    #[test]
    fn spec_has_info() {
        let spec = sample_spec().to_json();
        assert_eq!(spec["info"]["title"], "Test API");
        assert_eq!(spec["info"]["version"], "1.0.0");
        assert_eq!(spec["info"]["description"], "A test API");
    }

    #[test]
    fn spec_has_servers() {
        let spec = sample_spec().to_json();
        assert_eq!(spec["servers"][0]["url"], "http://localhost:3000");
        assert_eq!(spec["servers"][0]["description"], "Local");
    }

    #[test]
    fn spec_has_tags() {
        let spec = sample_spec().to_json();
        assert_eq!(spec["tags"][0]["name"], "users");
    }

    #[test]
    fn spec_has_paths() {
        let spec = sample_spec().to_json();
        assert!(spec["paths"]["/users"]["get"].is_object());
        assert!(spec["paths"]["/users"]["post"].is_object());
        assert!(spec["paths"]["/users/:id"]["get"].is_object());
        assert!(spec["paths"]["/users/:id"]["delete"].is_object());
    }

    #[test]
    fn route_has_summary_and_tags() {
        let spec = sample_spec().to_json();
        assert_eq!(spec["paths"]["/users"]["get"]["summary"], "List users");
        assert_eq!(spec["paths"]["/users"]["get"]["tags"][0], "users");
    }

    #[test]
    fn route_has_parameters() {
        let spec = sample_spec().to_json();
        let params = &spec["paths"]["/users"]["get"]["parameters"];
        assert_eq!(params[0]["name"], "page");
        assert_eq!(params[0]["in"], "query");
        assert_eq!(params[0]["schema"]["type"], "integer");
    }

    #[test]
    fn route_has_request_body() {
        let spec = sample_spec().to_json();
        let body = &spec["paths"]["/users"]["post"]["requestBody"];
        assert_eq!(body["required"], true);
        assert!(body["content"]["application/json"]["schema"]["$ref"]
            .as_str()
            .unwrap()
            .contains("CreateUser"));
    }

    #[test]
    fn route_has_responses() {
        let spec = sample_spec().to_json();
        let responses = &spec["paths"]["/users/:id"]["get"]["responses"];
        assert!(responses["200"].is_object());
        assert!(responses["404"].is_object());
    }

    #[test]
    fn deprecated_flag() {
        let spec = sample_spec().to_json();
        assert_eq!(
            spec["paths"]["/users/:id"]["delete"]["deprecated"],
            true
        );
    }

    #[test]
    fn spec_has_schemas() {
        let spec = sample_spec().to_json();
        let schemas = &spec["components"]["schemas"];
        assert!(schemas["User"].is_object());
        assert!(schemas["CreateUser"].is_object());
        assert_eq!(schemas["User"]["properties"]["email"]["format"], "email");
    }

    #[test]
    fn schema_builders() {
        assert_eq!(Schema::string().0["type"], "string");
        assert_eq!(Schema::integer().0["type"], "integer");
        assert_eq!(Schema::number().0["type"], "number");
        assert_eq!(Schema::boolean().0["type"], "boolean");
        assert_eq!(Schema::int64().0["format"], "int64");

        let arr = Schema::array(Schema::string());
        assert_eq!(arr.0["type"], "array");
        assert_eq!(arr.0["items"]["type"], "string");

        let r = Schema::ref_to("Foo");
        assert!(r.0["$ref"].as_str().unwrap().contains("Foo"));

        let e = Schema::enumeration(&["a", "b", "c"]);
        assert_eq!(e.0["enum"][0], "a");
    }

    #[test]
    fn schema_description_and_example() {
        let s = Schema::string()
            .description("A name")
            .example(json!("Alice"));
        assert_eq!(s.0["description"], "A name");
        assert_eq!(s.0["example"], "Alice");
    }

    #[test]
    fn schema_nullable() {
        let s = Schema::nullable(Schema::string());
        assert_eq!(s.0["nullable"], true);
        assert_eq!(s.0["type"], "string");
    }

    #[test]
    fn to_json_string_is_valid_json() {
        let json_str = sample_spec().to_json_string();
        let parsed: Value = serde_json::from_str(&json_str).unwrap();
        assert_eq!(parsed["openapi"], "3.1.0");
    }

    #[tokio::test]
    async fn json_endpoint_returns_spec() {
        let spec = sample_spec();
        let client = TestClient::new(
            Router::new().get("/openapi.json", spec.json_handler()),
        );

        let resp = client.get("/openapi.json").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.header("content-type").unwrap(), "application/json");

        let body = resp.text().await;
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["info"]["title"], "Test API");
    }

    #[tokio::test]
    async fn swagger_ui_returns_html() {
        let spec = sample_spec();
        let client = TestClient::new(
            Router::new().get("/docs", spec.swagger_ui()),
        );

        let resp = client.get("/docs").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(resp
            .header("content-type")
            .unwrap()
            .contains("text/html"));

        let body = resp.text().await;
        assert!(body.contains("swagger-ui"));
        assert!(body.contains("/openapi.json"));
    }

    #[tokio::test]
    async fn swagger_ui_custom_url() {
        let spec = sample_spec();
        let client = TestClient::new(
            Router::new().get("/docs", spec.swagger_ui_at("/api/spec.json")),
        );

        let resp = client.get("/docs").send().await;
        let body = resp.text().await;
        assert!(body.contains("/api/spec.json"));
    }

    #[test]
    fn empty_spec() {
        let spec = OpenApi::new("Empty", "0.0.0").to_json();
        assert_eq!(spec["info"]["title"], "Empty");
        assert!(spec["paths"].is_object());
    }

    #[test]
    fn operation_id() {
        let spec = OpenApi::new("API", "1.0.0")
            .route(ApiRoute::get("/test").operation_id("getTest"))
            .to_json();
        assert_eq!(spec["paths"]["/test"]["get"]["operationId"], "getTest");
    }

    // -- Router auto-registration tests -------------------------------------

    #[test]
    fn router_openapi_all_stubbed() {
        use crate::router::Router;

        let spec = Router::new()
            .get("/users", || async { "ok" })
            .post("/users", || async { "ok" })
            .delete("/users/:id", || async { "ok" })
            .openapi("Stub API", "0.1.0")
            .to_json();

        assert_eq!(spec["info"]["title"], "Stub API");
        assert!(spec["paths"]["/users"]["get"].is_object());
        assert!(spec["paths"]["/users"]["post"].is_object());
        assert!(spec["paths"]["/users/:id"]["delete"].is_object());
    }

    #[test]
    fn router_openapi_documented_route_uses_metadata() {
        use crate::router::Router;

        let spec = Router::new()
            .get("/users", || async { "ok" })
            .doc(
                ApiRoute::get("/users")
                    .summary("List all users")
                    .tag("users")
                    .response(200, "application/json", Schema::array(Schema::ref_to("User"))),
            )
            .openapi("Rich API", "1.0.0")
            .to_json();

        assert_eq!(spec["paths"]["/users"]["get"]["summary"], "List all users");
        assert_eq!(spec["paths"]["/users"]["get"]["tags"][0], "users");
        let schema_ref = &spec["paths"]["/users"]["get"]["responses"]["200"]
            ["content"]["application/json"]["schema"]["items"]["$ref"];
        assert!(schema_ref.as_str().unwrap().contains("User"));
    }

    #[test]
    fn router_openapi_mixed_documented_and_stub() {
        use crate::router::Router;

        let spec = Router::new()
            .get("/users", || async { "ok" })
            .doc(ApiRoute::get("/users").summary("Documented"))
            .post("/users", || async { "ok" })     // no doc → auto-stub
            .get("/health", || async { "ok" })     // no doc → auto-stub
            .openapi("Mixed API", "1.0.0")
            .to_json();

        // Documented route keeps its summary
        assert_eq!(spec["paths"]["/users"]["get"]["summary"], "Documented");
        // Undocumented routes appear as stubs (no summary key)
        assert!(spec["paths"]["/users"]["post"].is_object());
        assert!(spec["paths"]["/health"]["get"].is_object());
        // Stubs don't have a summary
        assert!(spec["paths"]["/users"]["post"]["summary"].is_null());
    }

    #[test]
    fn router_openapi_nested_routes_included() {
        use crate::router::Router;

        let api = Router::new()
            .get("/users", || async { "ok" })
            .doc(ApiRoute::get("/users").summary("List users"));

        let root = Router::new()
            .get("/health", || async { "ok" })
            .nest("/api", api)
            .openapi("Nested API", "1.0.0")
            .to_json();

        assert!(root["paths"]["/health"]["get"].is_object());
        assert!(root["paths"]["/api/users"]["get"].is_object());
        assert_eq!(root["paths"]["/api/users"]["get"]["summary"], "List users");
    }

    #[test]
    fn router_openapi_on_method_tracked() {
        use crate::router::Router;

        let spec = Router::new()
            .on("/resource", &[http::Method::GET, http::Method::HEAD], || async { "ok" })
            .openapi("On API", "1.0.0")
            .to_json();

        assert!(spec["paths"]["/resource"]["get"].is_object());
        assert!(spec["paths"]["/resource"]["head"].is_object());
    }

    #[test]
    fn api_route_with_prefix() {
        let r = ApiRoute::get("/users").summary("test");
        let prefixed = r.with_prefix("/api/v1");
        assert_eq!(prefixed.path(), "/api/v1/users");
        assert_eq!(prefixed.method(), "get");
        // summary is preserved
    }

    #[test]
    fn api_route_for_method() {
        let r = ApiRoute::for_method("patch", "/items/:id");
        assert_eq!(r.method(), "patch");
        assert_eq!(r.path(), "/items/:id");
    }

    // -- TypeScript codegen tests --------------------------------------------

    #[test]
    fn ts_codegen_contains_base_url() {
        let ts = sample_spec().generate_typescript("https://api.example.com");
        assert!(ts.contains("https://api.example.com"));
    }

    #[test]
    fn ts_codegen_contains_interface_for_schema() {
        let spec = OpenApi::new("API", "1.0.0")
            .schema(
                "User",
                Schema::object()
                    .property("id", Schema::integer())
                    .property("name", Schema::string())
                    .required(&["id", "name"])
                    .build(),
            );
        let ts = spec.generate_typescript("https://api.example.com");
        assert!(ts.contains("export interface User"));
        assert!(ts.contains("id: number"));
        assert!(ts.contains("name: string"));
    }

    #[test]
    fn ts_codegen_optional_field_uses_question_mark() {
        let spec = OpenApi::new("API", "1.0.0")
            .schema(
                "Item",
                Schema::object()
                    .property("id", Schema::integer())
                    .property("note", Schema::string()) // not in required → optional
                    .required(&["id"])
                    .build(),
            );
        let ts = spec.generate_typescript("https://api.example.com");
        assert!(ts.contains("note?: string"));
        // id is required — no question mark
        assert!(ts.contains("id: number"));
        assert!(!ts.contains("id?: number"));
    }

    #[test]
    fn ts_codegen_generates_get_function() {
        let spec = OpenApi::new("API", "1.0.0")
            .route(ApiRoute::get("/users").summary("List users")
                .response(200, "application/json", Schema::array(Schema::ref_to("User"))));
        let ts = spec.generate_typescript("https://api.example.com");
        assert!(ts.contains("export async function getUsers("));
        assert!(ts.contains("Promise<User[]>"));
        assert!(ts.contains("method: \"GET\""));
    }

    #[test]
    fn ts_codegen_path_param_in_function_signature() {
        let spec = OpenApi::new("API", "1.0.0")
            .route(
                ApiRoute::get("/users/:id")
                    .param(Parameter::path("id", Schema::integer()))
                    .response(200, "application/json", Schema::ref_to("User")),
            );
        let ts = spec.generate_typescript("https://api.example.com");
        assert!(ts.contains("getUsersById(id: number"));
        // Path template should use the param
        assert!(ts.contains("${id}"));
    }

    #[test]
    fn ts_codegen_post_with_body() {
        let spec = OpenApi::new("API", "1.0.0")
            .route(
                ApiRoute::post("/users")
                    .body("application/json", Schema::ref_to("CreateUser"))
                    .response(201, "application/json", Schema::ref_to("User")),
            );
        let ts = spec.generate_typescript("https://api.example.com");
        assert!(ts.contains("method: \"POST\""));
        assert!(ts.contains("JSON.stringify(body)"));
        assert!(ts.contains("body: CreateUser"));
    }

    #[test]
    fn ts_codegen_query_params_in_signature() {
        let spec = OpenApi::new("API", "1.0.0")
            .route(
                ApiRoute::get("/users")
                    .param(Parameter::query("page", Schema::integer()))
                    .param(Parameter::query("limit", Schema::integer()))
                    .response(200, "application/json", Schema::array(Schema::ref_to("User"))),
            );
        let ts = spec.generate_typescript("https://api.example.com");
        assert!(ts.contains("query?:"));
        assert!(ts.contains("page?"));
        assert!(ts.contains("URLSearchParams"));
    }

    #[test]
    fn ts_codegen_operation_id_overrides_name() {
        let spec = OpenApi::new("API", "1.0.0")
            .route(
                ApiRoute::get("/users")
                    .operation_id("listUsers")
                    .response(200, "application/json", Schema::array(Schema::ref_to("User"))),
            );
        let ts = spec.generate_typescript("https://api.example.com");
        assert!(ts.contains("export async function listUsers("));
    }

    #[test]
    fn ts_codegen_jsdoc_summary() {
        let spec = OpenApi::new("API", "1.0.0")
            .route(ApiRoute::get("/ping").summary("Health check"));
        let ts = spec.generate_typescript("https://api.example.com");
        assert!(ts.contains("/** Health check */"));
    }

    #[test]
    fn ts_codegen_void_return_no_json_parse() {
        let spec = OpenApi::new("API", "1.0.0")
            .route(ApiRoute::delete("/users/:id")
                .param(Parameter::path("id", Schema::integer())));
        let ts = spec.generate_typescript("https://api.example.com");
        assert!(ts.contains("Promise<void>"));
        assert!(!ts.contains("res.json()"));
    }

    #[test]
    fn ts_schema_to_type_ref() {
        let schema = Schema::ref_to("User");
        assert_eq!(schema_to_ts_type(&schema.0), "User");
    }

    #[test]
    fn ts_schema_to_type_array_of_ref() {
        let schema = Schema::array(Schema::ref_to("User"));
        assert_eq!(schema_to_ts_type(&schema.0), "User[]");
    }

    #[test]
    fn ts_path_template_no_params() {
        let t = path_to_ts_template("/users");
        assert!(t.contains("/users"));
        assert!(t.contains("BASE_URL"));
    }

    #[test]
    fn ts_path_template_with_param() {
        let t = path_to_ts_template("/users/:id/posts");
        assert!(t.contains("${id}"));
        assert!(t.contains("/posts"));
    }

    #[test]
    fn ts_function_name_simple() {
        assert_eq!(route_to_function_name("get", "/users"), "getUsers");
    }

    #[test]
    fn ts_function_name_with_path_param() {
        assert_eq!(route_to_function_name("get", "/users/:id"), "getUsersById");
    }

    #[test]
    fn ts_function_name_nested() {
        assert_eq!(route_to_function_name("delete", "/users/:id/posts/:postId"), "deleteUsersByIdPostsByPostId");
    }

    #[test]
    fn ts_codegen_type_alias_for_enum_schema() {
        let spec = OpenApi::new("API", "1.0.0")
            .schema("Status", Schema::enumeration(&["active", "inactive"]));
        let ts = spec.generate_typescript("https://api.example.com");
        assert!(ts.contains("export type Status ="));
    }
}
