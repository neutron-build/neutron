//! Minimal benchmark app for HTTP load testing.
//!
//! Run with:
//!   cargo run --release --example bench
//!
//! Then load test with wrk, hey, or oha:
//!   wrk -t4 -c100 -d10s http://127.0.0.1:3000/
//!   wrk -t4 -c100 -d10s http://127.0.0.1:3000/users/42
//!   wrk -t4 -c100 -d10s http://127.0.0.1:3000/json
//!
//! Or with hey:
//!   hey -n 100000 -c 100 http://127.0.0.1:3000/
//!   hey -n 100000 -c 100 http://127.0.0.1:3000/users/42
//!   hey -n 100000 -c 100 http://127.0.0.1:3000/json

use neutron::prelude::*;
use serde::Serialize;

#[derive(Serialize, Clone)]
struct User {
    id: u64,
    name: &'static str,
    email: &'static str,
}

async fn plaintext() -> &'static str {
    "Hello, World!"
}

async fn json() -> Json<User> {
    Json(User {
        id: 1,
        name: "Alice",
        email: "alice@example.com",
    })
}

async fn get_user(Path(id): Path<u64>) -> Json<User> {
    Json(User {
        id,
        name: "Alice",
        email: "alice@example.com",
    })
}

async fn not_found() -> (StatusCode, &'static str) {
    (StatusCode::NOT_FOUND, "Not Found")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let router = Router::new()
        .get("/", plaintext)
        .get("/json", json)
        .get("/users/:id", get_user)
        .fallback(not_found);

    println!("Benchmark server listening on http://127.0.0.1:3000");
    println!("Routes:");
    println!("  GET /          -> plaintext");
    println!("  GET /json      -> JSON");
    println!("  GET /users/:id -> JSON with path param");
    println!();
    println!("Load test with:");
    println!("  wrk -t4 -c100 -d10s http://127.0.0.1:3000/");

    Neutron::new().router(router).serve(3000).await
}
