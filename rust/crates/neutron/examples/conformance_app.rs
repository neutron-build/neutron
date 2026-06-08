//! Canonical Neutron conformance app (Rust SDK).
//!
//! Boots a Neutron Rust server with NO database so the cross-SDK conformance
//! runner can assert FRAMEWORK_CONTRACT.md against it. Mirrors the Go/Python/TS
//! conformance apps endpoint-for-endpoint:
//!
//!   GET  /health                       contract health shape via HealthCheck::contract
//!   GET  /openapi.json                 OpenAPI 3.1 document
//!   GET  /api/items                    200 list (compression / request-id probe)
//!   POST /api/items                    422 validation error (RFC 7807 + errors[])
//!   GET  /errors/{bad-request,…}       forced standard §2 errors
//!
//! Listen address comes from NEUTRON_HOST/NEUTRON_PORT (Config::from_env, contract
//! §6), so the runner pins an ephemeral port. Middleware follows the contract order.
//!
//! Run: `NEUTRON_PORT=8082 cargo run --release --example conformance_app`

use neutron::health::HealthCheck;
use neutron::openapi::{ApiRoute, OpenApi, Schema};
use neutron::prelude::*;
use neutron::validate::{Validate, ValidationErrors, Validated};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Clone)]
struct Item {
    id: u64,
    name: String,
    price: f64,
}

#[derive(Deserialize)]
struct NewItem {
    name: String,
    price: f64,
}

impl Validate for NewItem {
    fn validate(&self) -> Result<(), ValidationErrors> {
        let mut e = ValidationErrors::new();
        e.length("name", &self.name, 1, 200);
        e.range("price", self.price, 0.0, f64::MAX);
        e.into_result()
    }
}

async fn list_items() -> Json<Vec<Item>> {
    let items = (1..=50)
        .map(|i| Item {
            id: i,
            name: format!("conformance-item-{i}"),
            price: i as f64,
        })
        .collect();
    Json(items)
}

// Typed validation: a bad body returns the SDK's validation error response.
async fn create_item(Validated(Json(input)): Validated<Json<NewItem>>) -> (StatusCode, Json<Item>) {
    (
        StatusCode::CREATED,
        Json(Item { id: 1, name: input.name, price: input.price }),
    )
}

fn api_spec() -> OpenApi {
    OpenApi::new("Neutron Conformance API", "9.9.9")
        .route(
            ApiRoute::get("/api/items")
                .summary("List items")
                .response(
                    200,
                    "application/json",
                    Schema::array(Schema::ref_to("#/components/schemas/Item")),
                ),
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let spec = api_spec();
    let health = HealthCheck::new();

    let router = Router::new()
        // Contract middleware order: RequestID → Logging → … → CORS → Compression.
        .middleware(RequestId::new())
        .middleware(Logger::new())
        .middleware(Compress::new())
        .middleware(Cors::new().allow_any_origin().allow_any_method().allow_any_header())
        // Contract /health shape {status, nucleus, version}. No DB probe → unconfigured.
        .get("/health", health.contract(None, "9.9.9"))
        .get("/openapi.json", spec.json_handler())
        .get("/api/items", list_items)
        .post("/api/items", create_item)
        .get("/errors/bad-request", || async { AppError::bad_request("forced bad request") })
        .get("/errors/unauthorized", || async { AppError::unauthorized("forced unauthorized") })
        .get("/errors/forbidden", || async { AppError::forbidden("forced forbidden") })
        .get("/errors/not-found", || async { AppError::not_found("forced not found") })
        .get("/errors/conflict", || async { AppError::conflict("forced conflict") })
        .get("/errors/rate-limited", || async { AppError::rate_limited("forced rate limited") })
        .get("/errors/internal", || async { AppError::internal("forced internal error") });

    let config = Config::from_env();
    Neutron::new().router(router).listen(config.socket_addr()).await
}
