use criterion::{black_box, criterion_group, criterion_main, Criterion};
use http::Method;

use neutron::handler::{IntoResponse, Json, Request};
use neutron::router::Router;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_request(method: &Method, path: &str) -> Request {
    Request::new(
        method.clone(),
        path.parse().unwrap(),
        http::HeaderMap::new(),
        bytes::Bytes::new(),
    )
}

// ---------------------------------------------------------------------------
// Build a realistic router with many routes
// ---------------------------------------------------------------------------

fn build_router() -> Router {
    let mut router = Router::new()
        .get("/", || async { "home" })
        .get("/about", || async { "about" })
        .get("/health", || async { "ok" })
        .get("/api/v1/users", || async { "users" })
        .get("/api/v1/users/:id", || async { "user" })
        .post("/api/v1/users", || async { "create" })
        .put("/api/v1/users/:id", || async { "update" })
        .delete("/api/v1/users/:id", || async { "delete" })
        .get("/api/v1/posts", || async { "posts" })
        .get("/api/v1/posts/:id", || async { "post" })
        .get("/api/v1/posts/:id/comments", || async { "comments" })
        .post("/api/v1/posts/:id/comments", || async { "new_comment" })
        .get("/api/v1/orgs/:org/repos/:repo", || async { "repo" })
        .get("/api/v1/orgs/:org/repos/:repo/issues", || async { "issues" })
        .get(
            "/api/v1/orgs/:org/repos/:repo/issues/:id",
            || async { "issue" },
        )
        .get("/api/v2/graphql", || async { "graphql" })
        .get("/static/*", || async { "static" })
        .get("/docs/*", || async { "docs" });
    router.ensure_built();
    router
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

fn bench_static_root(c: &mut Criterion) {
    let router = build_router();
    c.bench_function("resolve: static root GET /", |b| {
        b.iter(|| {
            let _ = black_box(router.resolve(&Method::GET, "/"));
        })
    });
}

fn bench_static_deep(c: &mut Criterion) {
    let router = build_router();
    c.bench_function("resolve: static GET /api/v1/users", |b| {
        b.iter(|| {
            let _ = black_box(router.resolve(&Method::GET, "/api/v1/users"));
        })
    });
}

fn bench_param_single(c: &mut Criterion) {
    let router = build_router();
    c.bench_function("resolve: param GET /api/v1/users/:id", |b| {
        b.iter(|| {
            let _ = black_box(router.resolve(&Method::GET, "/api/v1/users/42"));
        })
    });
}

fn bench_param_multi(c: &mut Criterion) {
    let router = build_router();
    c.bench_function("resolve: multi-param GET /orgs/:org/repos/:repo", |b| {
        b.iter(|| {
            let _ = black_box(router.resolve(&Method::GET, "/api/v1/orgs/rust-lang/repos/rust"));
        })
    });
}

fn bench_param_deep(c: &mut Criterion) {
    let router = build_router();
    c.bench_function("resolve: deep param GET /orgs/:o/repos/:r/issues/:id", |b| {
        b.iter(|| {
            let _ = black_box(
                router.resolve(&Method::GET, "/api/v1/orgs/rust-lang/repos/rust/issues/123"),
            );
        })
    });
}

fn bench_wildcard(c: &mut Criterion) {
    let router = build_router();
    c.bench_function("resolve: wildcard GET /static/css/app.css", |b| {
        b.iter(|| {
            let _ = black_box(router.resolve(&Method::GET, "/static/css/app.css"));
        })
    });
}

fn bench_not_found(c: &mut Criterion) {
    let router = build_router();
    c.bench_function("resolve: not found GET /nonexistent/path", |b| {
        b.iter(|| {
            let _ = black_box(router.resolve(&Method::GET, "/nonexistent/path"));
        })
    });
}

fn bench_method_dispatch(c: &mut Criterion) {
    let router = build_router();
    c.bench_function("resolve: method POST /api/v1/users", |b| {
        b.iter(|| {
            let _ = black_box(router.resolve(&Method::POST, "/api/v1/users"));
        })
    });
}

fn bench_into_response_string(c: &mut Criterion) {
    c.bench_function("into_response: String", |b| {
        b.iter(|| {
            let _ = black_box(String::from("Hello, World!").into_response());
        })
    });
}

fn bench_into_response_json(c: &mut Criterion) {
    c.bench_function("into_response: Json", |b| {
        b.iter(|| {
            let val = serde_json::json!({"id": 42, "name": "Alice", "email": "alice@example.com"});
            let _ = black_box(Json(val).into_response());
        })
    });
}

fn bench_full_dispatch(c: &mut Criterion) {
    let router = build_router();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    c.bench_function("full dispatch: GET /api/v1/users/42", |b| {
        b.iter(|| {
            rt.block_on(async {
                let route = router.resolve(&Method::GET, "/api/v1/users/42").unwrap();
                let req = make_request(&Method::GET, "/api/v1/users/42");
                let _ = black_box(route.call(req).await);
            })
        })
    });
}

criterion_group!(
    benches,
    bench_static_root,
    bench_static_deep,
    bench_param_single,
    bench_param_multi,
    bench_param_deep,
    bench_wildcard,
    bench_not_found,
    bench_method_dispatch,
    bench_into_response_string,
    bench_into_response_json,
    bench_full_dispatch,
);
criterion_main!(benches);
