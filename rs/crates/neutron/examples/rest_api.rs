//! REST API example demonstrating JWT auth, validation, OpenAPI, and error handling.
//!
//! Run: `cargo run --example rest_api`
//!
//! Endpoints:
//!   POST /auth/login     — get a JWT token
//!   GET  /api/items      — list items (requires auth)
//!   GET  /api/items/:id  — get one item (requires auth)
//!   POST /api/items      — create item (requires auth, validated)
//!   GET  /health         — health check
//!   GET  /docs           — OpenAPI JSON spec

use std::sync::{Arc, Mutex};

use neutron::prelude::*;
use neutron::extract::Extension;
use neutron::jwt::{Claims, JwtAuth, JwtConfig};
use neutron::openapi::{ApiRoute, OpenApi, Parameter, Schema};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Domain types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Item {
    id: u64,
    name: String,
    price: f64,
}

#[derive(Debug, Deserialize)]
struct NewItem {
    name: String,
    price: f64,
}

#[derive(Debug, Serialize)]
struct TokenResponse {
    token: String,
}

// In-memory store
struct Store {
    items: Mutex<Vec<Item>>,
    next_id: Mutex<u64>,
}

impl Store {
    fn new() -> Self {
        Self {
            items: Mutex::new(vec![
                Item { id: 1, name: "Widget".into(), price: 9.99 },
                Item { id: 2, name: "Gadget".into(), price: 24.50 },
            ]),
            next_id: Mutex::new(3),
        }
    }
}

// ---------------------------------------------------------------------------
// JWT config (shared secret — in production, use env vars)
// ---------------------------------------------------------------------------

fn jwt_config() -> JwtConfig {
    JwtConfig::new(b"super-secret-key-change-me")
        .issuer("neutron-example")
}

// ---------------------------------------------------------------------------
// Auth handler — issues tokens
// ---------------------------------------------------------------------------

async fn login() -> Json<TokenResponse> {
    let config = jwt_config();
    let claims = Claims {
        sub: Some("user-1".into()),
        iss: Some("neutron-example".into()),
        exp: Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + 3600, // 1 hour
        ),
        ..Default::default()
    };
    let token = config.encode(&claims).expect("encoding should not fail");
    Json(TokenResponse { token })
}

// ---------------------------------------------------------------------------
// Protected handlers
// ---------------------------------------------------------------------------

async fn list_items(State(store): State<Arc<Store>>) -> Json<Vec<Item>> {
    let items = store.items.lock().unwrap().clone();
    Json(items)
}

async fn get_item(
    Path(id): Path<u64>,
    State(store): State<Arc<Store>>,
) -> Result<Json<Item>, (StatusCode, Json<serde_json::Value>)> {
    let items = store.items.lock().unwrap();
    items
        .iter()
        .find(|i| i.id == id)
        .cloned()
        .map(Json)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": "item not found" })),
            )
        })
}

async fn create_item(
    State(store): State<Arc<Store>>,
    Extension(claims): Extension<Claims>,
    Json(input): Json<NewItem>,
) -> (StatusCode, Json<Item>) {
    // Validate
    if input.name.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(Item { id: 0, name: "validation error: name required".into(), price: 0.0 }),
        );
    }

    let mut id_lock = store.next_id.lock().unwrap();
    let id = *id_lock;
    *id_lock += 1;

    let item = Item {
        id,
        name: input.name,
        price: input.price,
    };
    store.items.lock().unwrap().push(item.clone());

    tracing::info!(user = ?claims.sub, item_id = id, "item created");

    (StatusCode::CREATED, Json(item))
}

// ---------------------------------------------------------------------------
// OpenAPI spec
// ---------------------------------------------------------------------------

fn api_spec() -> OpenApi {
    OpenApi::new("Neutron REST API Example", "1.0.0")
        .description("Demonstrates JWT auth, CRUD, and validation")
        .server("/", None)
        .tag("auth", Some("Authentication"))
        .tag("items", Some("Item management"))
        .route(
            ApiRoute::post("/auth/login")
                .summary("Get a JWT token")
                .tag("auth")
                .response(200, "application/json", Schema::object().property("token", Schema::string()).build()),
        )
        .route(
            ApiRoute::get("/api/items")
                .summary("List all items")
                .tag("items")
                .response(200, "application/json", Schema::array(Schema::ref_to("#/components/schemas/Item"))),
        )
        .route(
            ApiRoute::get("/api/items/{id}")
                .summary("Get item by ID")
                .tag("items")
                .param(Parameter::path("id", Schema::integer()).description("Item ID"))
                .response(200, "application/json", Schema::ref_to("#/components/schemas/Item"))
                .response(404, "application/json", Schema::object().property("error", Schema::string()).build()),
        )
        .route(
            ApiRoute::post("/api/items")
                .summary("Create a new item")
                .tag("items")
                .body(
                    "application/json",
                    Schema::object()
                        .property("name", Schema::string())
                        .property("price", Schema::number())
                        .build(),
                )
                .response(201, "application/json", Schema::ref_to("#/components/schemas/Item")),
        )
        .schema(
            "Item",
            Schema::object()
                .property("id", Schema::integer())
                .property("name", Schema::string())
                .property("price", Schema::number())
                .build(),
        )
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    let store = Arc::new(Store::new());
    let spec = api_spec();

    // Protected API routes (JWT required)
    let api = Router::new()
        .middleware(JwtAuth::new(jwt_config()))
        .get("/items", list_items)
        .get("/items/:id", get_item)
        .post("/items", create_item);

    let router = Router::new()
        .state(store)
        .middleware(Logger)
        .middleware(RequestId::new())
        .middleware(Cors::new().allow_any_origin().allow_any_method().allow_any_header())
        .post("/auth/login", login)
        .nest("/api", api)
        .get("/health", || async { Json(serde_json::json!({ "status": "ok" })) })
        .get("/docs", spec.json_handler())
        .fallback(|| async {
            (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "not found" })))
        });

    let addr = "0.0.0.0:3000".parse()?;
    Neutron::new().router(router).listen(addr).await
}
