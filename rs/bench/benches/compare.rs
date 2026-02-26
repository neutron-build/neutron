//! Competitive HTTP framework benchmarks.
//!
//! Measures real end-to-end HTTP/1.1 latency: TCP accept, routing, body
//! parsing, serialisation, and response write.  Each framework runs on its
//! own OS-assigned port in a background thread; a shared `reqwest` blocking
//! client hammers all of them with identical requests.
//!
//! Run all groups:
//!   cargo bench --bench compare
//!
//! Run a single group:
//!   cargo bench --bench compare -- plaintext
//!
//! HTML reports land in target/criterion/.

use std::net::SocketAddr;
use std::sync::OnceLock;
use std::thread;
use std::time::Duration;

use axum::response::IntoResponse as AxumIntoResponse;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Shared types (prefixed "Bench" to avoid conflicts with framework re-exports
// such as tungstenite::Message)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchUser {
    id: u64,
    name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchMessage {
    message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NewBenchUser {
    name: String,
}

// ---------------------------------------------------------------------------
// Shared HTTP client (one persistent connection pool for all frameworks)
// ---------------------------------------------------------------------------

fn client() -> &'static reqwest::blocking::Client {
    static CLIENT: OnceLock<reqwest::blocking::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::blocking::Client::builder()
            .pool_max_idle_per_host(1)
            .connection_verbose(false)
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap()
    })
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Block until the server responds to GET /plaintext (max 3 s).
fn wait_ready(addr: SocketAddr) {
    let url = format!("http://{addr}/plaintext");
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        if reqwest::blocking::get(&url).is_ok() {
            return;
        }
        if std::time::Instant::now() > deadline {
            panic!("server at {addr} did not become ready within 3s");
        }
        thread::sleep(Duration::from_millis(20));
    }
}

/// Bind `127.0.0.1:0` and return the OS-assigned port.
fn random_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

// ---------------------------------------------------------------------------
// neutron server
// ---------------------------------------------------------------------------

fn start_neutron() -> SocketAddr {
    use neutron::prelude::*;

    let port = random_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async move {
            // Middleware is scoped ONLY to /middleware via a nested sub-router.
            // All other routes (plaintext, json, path_param, json_body) run
            // zero middleware — the same as the other frameworks — for a fair
            // apples-to-apples comparison of routing + extraction speed.
            let router = Router::new()
                .get("/plaintext", || async { "Hello, World!" })
                .get("/json", || async {
                    Json(BenchMessage { message: "hello".into() })
                })
                .get("/users/:id", |Path(id): Path<u64>| async move {
                    Json(BenchUser { id, name: "Alice".into() })
                })
                .post("/users", |Json(body): Json<NewBenchUser>| async move {
                    Json(BenchUser { id: 1, name: body.name })
                })
                .nest("", Router::new()
                    .middleware(Logger::new())
                    .middleware(RequestId::new())
                    .middleware(Timeout::from_secs(5))
                    .get("/middleware", || async { "ok" })
                );

            Neutron::new()
                .shutdown_signal(std::future::pending())
                .router(router)
                .listen(addr)
                .await
                .ok();
        });
    });

    wait_ready(addr);
    addr
}

// ---------------------------------------------------------------------------
// axum server
// ---------------------------------------------------------------------------

fn start_axum() -> SocketAddr {
    use axum::extract::Json as AxumJson;
    use axum::extract::Path as AxumPath;
    use axum::extract::Request as AxumReq;
    use axum::middleware::{self as axum_mw, Next};
    use axum::response::Response as AxumResp;
    use axum::{routing, Router as AxumRouter};

    // Three axum middleware functions equivalent to neutron's Logger, RequestId,
    // and Timeout — each adds one Box::pin async layer, matching neutron's cost.
    async fn axum_logger(req: AxumReq, next: Next) -> AxumResp {
        let method = req.method().clone();
        let uri = req.uri().clone();
        let resp = next.run(req).await;
        let _ = (method, uri); // suppress unused warning; real logger would log here
        resp
    }
    async fn axum_request_id(mut req: AxumReq, next: Next) -> AxumResp {
        req.headers_mut().insert("x-request-id", "bench".parse().unwrap());
        next.run(req).await
    }
    async fn axum_timeout(req: AxumReq, next: Next) -> AxumResp {
        tokio::time::timeout(Duration::from_secs(5), next.run(req))
            .await
            .unwrap_or_else(|_| axum::http::StatusCode::REQUEST_TIMEOUT.into_response())
    }

    let port = random_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async move {
            let app = AxumRouter::new()
                .route("/plaintext", routing::get(|| async { "Hello, World!" }))
                .route(
                    "/json",
                    routing::get(|| async {
                        AxumJson(BenchMessage { message: "hello".into() })
                    }),
                )
                .route(
                    "/users/:id",
                    routing::get(|AxumPath(id): AxumPath<u64>| async move {
                        AxumJson(BenchUser { id, name: "Alice".into() })
                    }),
                )
                .route(
                    "/users",
                    routing::post(|AxumJson(body): AxumJson<NewBenchUser>| async move {
                        AxumJson(BenchUser { id: 1, name: body.name })
                    }),
                )
                // /middleware gets 3 axum middleware layers (Logger+RequestId+Timeout
                // equivalent) so the comparison with neutron is apples-to-apples.
                .route(
                    "/middleware",
                    routing::get(|| async { "ok" })
                        .layer(axum_mw::from_fn(axum_logger))
                        .layer(axum_mw::from_fn(axum_request_id))
                        .layer(axum_mw::from_fn(axum_timeout)),
                );

            let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
            axum::serve(listener, app).await.unwrap();
        });
    });

    wait_ready(addr);
    addr
}

// ---------------------------------------------------------------------------
// actix-web server
// ---------------------------------------------------------------------------

fn start_actix() -> SocketAddr {
    use actix_web::{web, App, HttpResponse, HttpServer};

    let port = random_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let addr_str = addr.to_string();

    thread::spawn(move || {
        let sys = actix_web::rt::System::new();
        sys.block_on(async move {
            HttpServer::new(|| {
                App::new()
                    .route(
                        "/plaintext",
                        web::get().to(|| async { HttpResponse::Ok().body("Hello, World!") }),
                    )
                    .route(
                        "/json",
                        web::get().to(|| async {
                            HttpResponse::Ok()
                                .json(BenchMessage { message: "hello".into() })
                        }),
                    )
                    .route(
                        "/users/{id}",
                        web::get().to(|path: web::Path<u64>| async move {
                            HttpResponse::Ok().json(BenchUser {
                                id: path.into_inner(),
                                name: "Alice".into(),
                            })
                        }),
                    )
                    .route(
                        "/users",
                        web::post().to(|body: web::Json<NewBenchUser>| async move {
                            HttpResponse::Ok()
                                .json(BenchUser { id: 1, name: body.into_inner().name })
                        }),
                    )
                    .route(
                        "/middleware",
                        web::get().to(|| async { HttpResponse::Ok().body("ok") }),
                    )
            })
            .bind(&addr_str)
            .unwrap()
            .run()
            .await
            .unwrap();
        });
    });

    wait_ready(addr);
    addr
}

// ---------------------------------------------------------------------------
// warp server
// ---------------------------------------------------------------------------

fn start_warp() -> SocketAddr {
    use warp::Filter;

    let port = random_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async move {
            let plaintext = warp::path("plaintext")
                .and(warp::get())
                .map(|| "Hello, World!");

            let json_route = warp::path("json").and(warp::get()).map(|| {
                warp::reply::json(&BenchMessage { message: "hello".into() })
            });

            let get_user = warp::path!("users" / u64)
                .and(warp::get())
                .map(|id: u64| warp::reply::json(&BenchUser { id, name: "Alice".into() }));

            let post_user = warp::path("users")
                .and(warp::post())
                .and(warp::body::json())
                .map(|body: NewBenchUser| {
                    warp::reply::json(&BenchUser { id: 1, name: body.name })
                });

            let middleware_route =
                warp::path("middleware").and(warp::get()).map(|| "ok");

            let routes = plaintext
                .or(json_route)
                .or(get_user)
                .or(post_user)
                .or(middleware_route);

            warp::serve(routes).run(addr).await;
        });
    });

    wait_ready(addr);
    addr
}

// ---------------------------------------------------------------------------
// Raw hyper server (zero-framework baseline)
// ---------------------------------------------------------------------------

fn start_hyper_raw() -> SocketAddr {
    use bytes::Bytes;
    use http::{Response as HyperResponse, StatusCode};
    use http_body_util::{BodyExt, Full};
    use hyper::body::Incoming;
    use hyper::service::service_fn;
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use hyper_util::server::conn::auto::Builder;

    let port = random_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async move {
            let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    let service = service_fn(|req: hyper::Request<Incoming>| async move {
                        let path = req.uri().path().to_string();
                        let method = req.method().clone();

                        let resp: HyperResponse<Full<Bytes>> =
                            match (method.as_str(), path.as_str()) {
                                ("GET", "/plaintext") => HyperResponse::builder()
                                    .header("content-type", "text/plain; charset=utf-8")
                                    .body(Full::new(Bytes::from_static(b"Hello, World!")))
                                    .unwrap(),
                                ("GET", "/json") => {
                                    let body = serde_json::to_vec(&BenchMessage {
                                        message: "hello".into(),
                                    })
                                    .unwrap();
                                    HyperResponse::builder()
                                        .header("content-type", "application/json")
                                        .body(Full::new(Bytes::from(body)))
                                        .unwrap()
                                }
                                ("GET", p) if p.starts_with("/users/") => {
                                    let id: u64 =
                                        p.trim_start_matches("/users/").parse().unwrap_or(0);
                                    let body = serde_json::to_vec(&BenchUser {
                                        id,
                                        name: "Alice".into(),
                                    })
                                    .unwrap();
                                    HyperResponse::builder()
                                        .header("content-type", "application/json")
                                        .body(Full::new(Bytes::from(body)))
                                        .unwrap()
                                }
                                ("POST", "/users") => {
                                    let body_bytes =
                                        req.into_body().collect().await.unwrap().to_bytes();
                                    let new_user: NewBenchUser =
                                        serde_json::from_slice(&body_bytes)
                                            .unwrap_or(NewBenchUser { name: "unknown".into() });
                                    let body = serde_json::to_vec(&BenchUser {
                                        id: 1,
                                        name: new_user.name,
                                    })
                                    .unwrap();
                                    HyperResponse::builder()
                                        .header("content-type", "application/json")
                                        .body(Full::new(Bytes::from(body)))
                                        .unwrap()
                                }
                                ("GET", "/middleware") => HyperResponse::builder()
                                    .header("content-type", "text/plain; charset=utf-8")
                                    .body(Full::new(Bytes::from_static(b"ok")))
                                    .unwrap(),
                                _ => HyperResponse::builder()
                                    .status(StatusCode::NOT_FOUND)
                                    .body(Full::new(Bytes::from_static(b"Not Found")))
                                    .unwrap(),
                            };

                        Ok::<_, std::convert::Infallible>(resp)
                    });

                    Builder::new(TokioExecutor::new())
                        .serve_connection(TokioIo::new(stream), service)
                        .await
                        .ok();
                });
            }
        });
    });

    wait_ready(addr);
    addr
}

// ---------------------------------------------------------------------------
// Lazily-started server addresses (started once, reused across all benches)
// ---------------------------------------------------------------------------

fn neutron_addr() -> SocketAddr {
    static ADDR: OnceLock<SocketAddr> = OnceLock::new();
    *ADDR.get_or_init(start_neutron)
}

fn axum_addr() -> SocketAddr {
    static ADDR: OnceLock<SocketAddr> = OnceLock::new();
    *ADDR.get_or_init(start_axum)
}

fn actix_addr() -> SocketAddr {
    static ADDR: OnceLock<SocketAddr> = OnceLock::new();
    *ADDR.get_or_init(start_actix)
}

fn warp_addr() -> SocketAddr {
    static ADDR: OnceLock<SocketAddr> = OnceLock::new();
    *ADDR.get_or_init(start_warp)
}

fn hyper_raw_addr() -> SocketAddr {
    static ADDR: OnceLock<SocketAddr> = OnceLock::new();
    *ADDR.get_or_init(start_hyper_raw)
}

// ---------------------------------------------------------------------------
// Benchmark helpers
// ---------------------------------------------------------------------------

fn all_servers() -> [(&'static str, SocketAddr); 5] {
    [
        ("neutron",   neutron_addr()),
        ("axum",      axum_addr()),
        ("actix-web", actix_addr()),
        ("warp",      warp_addr()),
        ("hyper-raw", hyper_raw_addr()),
    ]
}

// ---------------------------------------------------------------------------
// Benchmark: GET /plaintext → "Hello, World!"
// ---------------------------------------------------------------------------

fn bench_plaintext(c: &mut Criterion) {
    let mut group = c.benchmark_group("plaintext");
    group.measurement_time(Duration::from_secs(8));
    group.sample_size(200);

    for (name, addr) in all_servers() {
        let url = format!("http://{addr}/plaintext");
        group.bench_with_input(BenchmarkId::from_parameter(name), &url, |b, url| {
            b.iter(|| {
                client().get(url).send().unwrap();
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: GET /json → {"message":"hello"}
// ---------------------------------------------------------------------------

fn bench_json(c: &mut Criterion) {
    let mut group = c.benchmark_group("json");
    group.measurement_time(Duration::from_secs(8));
    group.sample_size(200);

    for (name, addr) in all_servers() {
        let url = format!("http://{addr}/json");
        group.bench_with_input(BenchmarkId::from_parameter(name), &url, |b, url| {
            b.iter(|| {
                client().get(url).send().unwrap();
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: GET /users/42 → {"id":42,"name":"Alice"}
// ---------------------------------------------------------------------------

fn bench_path_param(c: &mut Criterion) {
    let mut group = c.benchmark_group("path_param");
    group.measurement_time(Duration::from_secs(8));
    group.sample_size(200);

    for (name, addr) in all_servers() {
        let url = format!("http://{addr}/users/42");
        group.bench_with_input(BenchmarkId::from_parameter(name), &url, |b, url| {
            b.iter(|| {
                client().get(url).send().unwrap();
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: POST /users with JSON body → echo user
// ---------------------------------------------------------------------------

fn bench_json_body(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_body");
    group.measurement_time(Duration::from_secs(8));
    group.sample_size(200);

    let payload = NewBenchUser { name: "Bob".into() };

    for (name, addr) in all_servers() {
        let url = format!("http://{addr}/users");
        group.bench_with_input(BenchmarkId::from_parameter(name), &url, |b, url| {
            b.iter(|| {
                client().post(url).json(&payload).send().unwrap();
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: GET /middleware
//
// All frameworks run through an equivalent 3-layer middleware stack:
//   neutron : Logger + RequestId + Timeout  (scoped to /middleware only)
//   axum    : TraceLayer + SetRequestId + TimeoutLayer (Tower)
//   actix   : plain handler (no middleware — shows raw framework overhead)
//   warp    : plain handler (no middleware — shows raw framework overhead)
//   hyper   : plain handler (zero-framework baseline)
//
// Comparing neutron vs axum shows the cost of neutron's vs Tower middleware.
// Comparing neutron vs actix/warp/hyper shows middleware cost vs bare handler.
// ---------------------------------------------------------------------------

fn bench_middleware(c: &mut Criterion) {
    let mut group = c.benchmark_group("middleware");
    group.measurement_time(Duration::from_secs(8));
    group.sample_size(200);

    for (name, addr) in all_servers() {
        let url = format!("http://{addr}/middleware");
        group.bench_with_input(BenchmarkId::from_parameter(name), &url, |b, url| {
            b.iter(|| {
                client().get(url).send().unwrap();
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Criterion groups
// ---------------------------------------------------------------------------

criterion_group!(
    benches,
    bench_plaintext,
    bench_json,
    bench_path_param,
    bench_json_body,
    bench_middleware,
);
criterion_main!(benches);
