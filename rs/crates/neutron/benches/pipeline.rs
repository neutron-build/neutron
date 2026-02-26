use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

use http::{HeaderMap, Method};
use serde::{Deserialize, Serialize};

use neutron::extract::{Path, Query};
use neutron::handler::{IntoResponse, Json, Request, Response};
use neutron::logger::Logger;
use neutron::middleware::Next;
use neutron::negotiate::AcceptHeader;
use neutron::request_id::RequestId;
use neutron::router::Router;
use neutron::testing::TestClient;
use neutron::timeout::Timeout;

// ---------------------------------------------------------------------------
// Sample types
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
struct User {
    id: u64,
    name: String,
    email: String,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct ListParams {
    page: Option<u32>,
    limit: Option<u32>,
}

// ---------------------------------------------------------------------------
// 1. Full pipeline: plaintext
// ---------------------------------------------------------------------------

fn bench_pipeline_plaintext(c: &mut Criterion) {
    let client = TestClient::new(Router::new().get("/", || async { "Hello, World!" }));

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    c.bench_function("pipeline: plaintext GET /", |b| {
        b.iter(|| {
            rt.block_on(async {
                let resp = client.get("/").send().await;
                black_box(resp);
            })
        })
    });
}

// ---------------------------------------------------------------------------
// 2. Full pipeline: JSON response
// ---------------------------------------------------------------------------

fn bench_pipeline_json(c: &mut Criterion) {
    let client = TestClient::new(Router::new().get("/user", || async {
        Json(User {
            id: 42,
            name: "Alice".into(),
            email: "alice@example.com".into(),
        })
    }));

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    c.bench_function("pipeline: JSON GET /user", |b| {
        b.iter(|| {
            rt.block_on(async {
                let resp = client.get("/user").send().await;
                black_box(resp);
            })
        })
    });
}

// ---------------------------------------------------------------------------
// 3. Full pipeline: path param + JSON
// ---------------------------------------------------------------------------

fn bench_pipeline_path_param(c: &mut Criterion) {
    let client = TestClient::new(
        Router::new().get("/users/:id", |Path(id): Path<u64>| async move {
            Json(User {
                id,
                name: "Alice".into(),
                email: "alice@example.com".into(),
            })
        }),
    );

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    c.bench_function("pipeline: path param GET /users/42 -> JSON", |b| {
        b.iter(|| {
            rt.block_on(async {
                let resp = client.get("/users/42").send().await;
                black_box(resp);
            })
        })
    });
}

// ---------------------------------------------------------------------------
// 4. Full pipeline: JSON body extraction
// ---------------------------------------------------------------------------

fn bench_pipeline_json_body(c: &mut Criterion) {
    let client = TestClient::new(
        Router::new().post("/users", |Json(user): Json<User>| async move { Json(user) }),
    );

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    c.bench_function("pipeline: JSON body POST /users", |b| {
        b.iter(|| {
            rt.block_on(async {
                let resp = client
                    .post("/users")
                    .json(&User {
                        id: 1,
                        name: "Bob".into(),
                        email: "bob@example.com".into(),
                    })
                    .send()
                    .await;
                black_box(resp);
            })
        })
    });
}

// ---------------------------------------------------------------------------
// 5. Full pipeline: query string extraction
// ---------------------------------------------------------------------------

fn bench_pipeline_query(c: &mut Criterion) {
    let client = TestClient::new(
        Router::new().get("/items", |Query(_params): Query<ListParams>| async {
            Json(serde_json::json!({ "items": [] }))
        }),
    );

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    c.bench_function("pipeline: query string GET /items?page=2&limit=20", |b| {
        b.iter(|| {
            rt.block_on(async {
                let resp = client.get("/items?page=2&limit=20").send().await;
                black_box(resp);
            })
        })
    });
}

// ---------------------------------------------------------------------------
// 6. Middleware overhead: 0, 1, 3, 5 middleware
// ---------------------------------------------------------------------------

async fn noop_middleware(req: Request, next: Next) -> Response {
    next.run(req).await
}

fn bench_middleware_overhead(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("middleware overhead");

    for count in [0, 1, 3, 5] {
        let mut router = Router::new();
        for _ in 0..count {
            router = router.middleware(noop_middleware);
        }
        router = router.get("/", || async { "ok" });
        let client = TestClient::new(router);

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{count} noop")),
            &count,
            |b, _| {
                b.iter(|| {
                    rt.block_on(async {
                        let resp = client.get("/").send().await;
                        black_box(resp);
                    })
                })
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 7. Real middleware chain: logger + request-id + timeout
// ---------------------------------------------------------------------------

fn bench_real_middleware_chain(c: &mut Criterion) {
    let client = TestClient::new(
        Router::new()
            .middleware(RequestId::new())
            .middleware(Logger)
            .middleware(Timeout::new(std::time::Duration::from_secs(30)))
            .get("/", || async { "ok" }),
    );

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    c.bench_function("pipeline: 3 real middleware (reqid+logger+timeout)", |b| {
        b.iter(|| {
            rt.block_on(async {
                let resp = client.get("/").send().await;
                black_box(resp);
            })
        })
    });
}

// ---------------------------------------------------------------------------
// 8. IntoResponse conversion overhead
// ---------------------------------------------------------------------------

fn bench_into_response(c: &mut Criterion) {
    let mut group = c.benchmark_group("into_response");

    group.bench_function("&str", |b| {
        b.iter(|| black_box("Hello, World!".into_response()))
    });

    group.bench_function("String", |b| {
        b.iter(|| black_box(String::from("Hello, World!").into_response()))
    });

    group.bench_function("Json(small)", |b| {
        b.iter(|| {
            black_box(Json(serde_json::json!({"id": 42, "name": "Alice"})).into_response())
        })
    });

    group.bench_function("Json(medium)", |b| {
        b.iter(|| {
            let users: Vec<User> = (0..10)
                .map(|i| User {
                    id: i,
                    name: format!("User {i}"),
                    email: format!("user{i}@example.com"),
                })
                .collect();
            black_box(Json(users).into_response())
        })
    });

    group.bench_function("(StatusCode, &str)", |b| {
        b.iter(|| black_box((http::StatusCode::OK, "Hello, World!").into_response()))
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 9. Content negotiation
// ---------------------------------------------------------------------------

fn bench_content_negotiation(c: &mut Criterion) {
    let mut group = c.benchmark_group("content negotiation");

    group.bench_function("parse simple Accept", |b| {
        b.iter(|| {
            black_box(AcceptHeader::parse("application/json"));
        })
    });

    group.bench_function("parse complex Accept", |b| {
        b.iter(|| {
            black_box(AcceptHeader::parse(
                "text/html;q=0.9, application/json;q=1.0, application/xml;q=0.5, */*;q=0.1",
            ));
        })
    });

    group.bench_function("negotiate 4 types", |b| {
        b.iter(|| {
            black_box(neutron::negotiate::negotiate(
                "text/html;q=0.9, application/json;q=1.0, application/xml;q=0.5, */*;q=0.1",
                &[
                    "application/json",
                    "text/html",
                    "application/xml",
                    "text/plain",
                ],
            ));
        })
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 10. Router at scale: 10, 100, 500 routes
// ---------------------------------------------------------------------------

fn build_large_router(n: usize) -> Router {
    let mut router = Router::new();
    for i in 0..n {
        let path: &'static str = Box::leak(format!("/api/v1/resource{i}/:id").into_boxed_str());
        router = router.get(path, || async { "ok" });
    }
    router.ensure_built();
    router
}

fn bench_router_scale(c: &mut Criterion) {
    let mut group = c.benchmark_group("router scale");

    for route_count in [10, 100, 500] {
        let router = build_large_router(route_count);

        group.bench_with_input(
            BenchmarkId::new("first route", route_count),
            &route_count,
            |b, _| {
                b.iter(|| {
                    let _ = black_box(router.resolve(&Method::GET, "/api/v1/resource0/42"));
                })
            },
        );

        let last_path = format!("/api/v1/resource{}/42", route_count - 1);
        group.bench_with_input(
            BenchmarkId::new("last route", route_count),
            &route_count,
            |b, _| {
                b.iter(|| {
                    let _ = black_box(router.resolve(&Method::GET, &last_path));
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("miss (404)", route_count),
            &route_count,
            |b, _| {
                b.iter(|| {
                    let _ = black_box(router.resolve(&Method::GET, "/nonexistent/path"));
                })
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 11. Extractor overhead in isolation
// ---------------------------------------------------------------------------

fn bench_extractors(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("extractors");

    // Path<u64>
    let client_path = TestClient::new(
        Router::new().get("/items/:id", |Path(id): Path<u64>| async move { id.to_string() }),
    );
    group.bench_function("Path<u64>", |b| {
        b.iter(|| {
            rt.block_on(async {
                black_box(client_path.get("/items/42").send().await);
            })
        })
    });

    // Query<T>
    let client_query = TestClient::new(
        Router::new().get("/items", |Query(_p): Query<ListParams>| async { "ok" }),
    );
    group.bench_function("Query<ListParams>", |b| {
        b.iter(|| {
            rt.block_on(async {
                black_box(client_query.get("/items?page=1&limit=20").send().await);
            })
        })
    });

    // Json<T> body
    let client_json = TestClient::new(
        Router::new().post("/users", |Json(_u): Json<User>| async { "ok" }),
    );
    group.bench_function("Json<User> body", |b| {
        b.iter(|| {
            rt.block_on(async {
                black_box(
                    client_json
                        .post("/users")
                        .json(&User {
                            id: 1,
                            name: "Alice".into(),
                            email: "alice@example.com".into(),
                        })
                        .send()
                        .await,
                );
            })
        })
    });

    // HeaderMap
    let client_headers = TestClient::new(
        Router::new().get("/", |_h: HeaderMap| async { "ok" }),
    );
    group.bench_function("HeaderMap", |b| {
        b.iter(|| {
            rt.block_on(async {
                black_box(
                    client_headers
                        .get("/")
                        .header("x-request-id", "abc123")
                        .header("accept", "application/json")
                        .send()
                        .await,
                );
            })
        })
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Groups
// ---------------------------------------------------------------------------

criterion_group!(
    pipeline,
    bench_pipeline_plaintext,
    bench_pipeline_json,
    bench_pipeline_path_param,
    bench_pipeline_json_body,
    bench_pipeline_query,
);

criterion_group!(middleware, bench_middleware_overhead, bench_real_middleware_chain);

criterion_group!(responses, bench_into_response);

criterion_group!(negotiation, bench_content_negotiation);

criterion_group!(scale, bench_router_scale);

criterion_group!(extractors, bench_extractors);

criterion_main!(pipeline, middleware, responses, negotiation, scale, extractors);
