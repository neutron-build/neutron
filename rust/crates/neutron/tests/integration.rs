//! Integration tests that start a real TCP server and make HTTP requests.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio::sync::oneshot;

use neutron::prelude::*;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Start a Neutron server with a custom body size limit.
async fn start_server_with_body_limit(
    router: Router,
    max_body: usize,
) -> (SocketAddr, oneshot::Sender<()>) {
    let (tx, rx) = oneshot::channel::<()>();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let _handle = tokio::spawn(async move {
        Neutron::new()
            .shutdown_signal(async move {
                rx.await.ok();
            })
            .shutdown_timeout(Duration::from_secs(1))
            .max_body_size(max_body)
            .router(router)
            .listen(addr)
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    for _ in 0..20 {
        if TcpStream::connect(addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    (addr, tx)
}

/// Start a Neutron server on a random port and return the address + shutdown sender.
async fn start_server(router: Router) -> (SocketAddr, oneshot::Sender<()>) {
    let (tx, rx) = oneshot::channel::<()>();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // Drop the listener so Neutron can bind to the same port
    drop(listener);

    let _handle = tokio::spawn(async move {
        Neutron::new()
            .shutdown_signal(async move {
                rx.await.ok();
            })
            .shutdown_timeout(Duration::from_secs(1))
            .router(router)
            .listen(addr)
            .await
            .unwrap();
    });

    // Give the server a moment to start listening
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Verify the server is actually listening by trying to connect
    for _ in 0..20 {
        if TcpStream::connect(addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    (addr, tx)
}

/// Make a GET request to the given address and path using raw hyper client.
async fn http_get(addr: SocketAddr, path: &str) -> hyper::Response<Incoming> {
    let stream = TcpStream::connect(addr).await.unwrap();
    let io = TokioIo::new(stream);

    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await.unwrap();
    tokio::spawn(conn);

    let req = hyper::Request::builder()
        .method("GET")
        .uri(path)
        .header("host", format!("127.0.0.1:{}", addr.port()))
        .body(Full::new(Bytes::new()))
        .unwrap();

    sender.send_request(req).await.unwrap()
}

/// Make a POST request with a JSON body.
async fn http_post_json(addr: SocketAddr, path: &str, body: &str) -> hyper::Response<Incoming> {
    let stream = TcpStream::connect(addr).await.unwrap();
    let io = TokioIo::new(stream);

    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await.unwrap();
    tokio::spawn(conn);

    let req = hyper::Request::builder()
        .method("POST")
        .uri(path)
        .header("host", format!("127.0.0.1:{}", addr.port()))
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(body.to_string())))
        .unwrap();

    sender.send_request(req).await.unwrap()
}

/// Read the full body as a string.
async fn body_string(resp: hyper::Response<Incoming>) -> String {
    let body_bytes = resp.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8(body_bytes.to_vec()).unwrap()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn basic_get() {
    let router = Router::new().get("/", || async { "hello from neutron" });

    let (addr, shutdown) = start_server(router).await;

    let resp = http_get(addr, "/").await;
    assert_eq!(resp.status(), 200);
    assert_eq!(body_string(resp).await, "hello from neutron");

    shutdown.send(()).ok();
}

#[tokio::test]
async fn json_response() {
    let router = Router::new().get("/data", || async {
        Json(serde_json::json!({ "key": "value", "num": 42 }))
    });

    let (addr, shutdown) = start_server(router).await;

    let resp = http_get(addr, "/data").await;
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap(),
        "application/json"
    );

    let body = body_string(resp).await;
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["key"], "value");
    assert_eq!(json["num"], 42);

    shutdown.send(()).ok();
}

#[tokio::test]
async fn path_params() {
    let router = Router::new().get("/users/:id", |Path(id): Path<u64>| async move {
        Json(serde_json::json!({ "user_id": id }))
    });

    let (addr, shutdown) = start_server(router).await;

    let resp = http_get(addr, "/users/42").await;
    assert_eq!(resp.status(), 200);

    let body = body_string(resp).await;
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["user_id"], 42);

    shutdown.send(()).ok();
}

#[tokio::test]
async fn post_json_body() {
    #[derive(Deserialize)]
    struct Input {
        name: String,
    }

    #[derive(Serialize)]
    struct Output {
        greeting: String,
    }

    let router = Router::new().post("/greet", |Json(input): Json<Input>| async move {
        Json(Output {
            greeting: format!("Hello, {}!", input.name),
        })
    });

    let (addr, shutdown) = start_server(router).await;

    let resp = http_post_json(addr, "/greet", r#"{"name":"World"}"#).await;
    assert_eq!(resp.status(), 200);

    let body = body_string(resp).await;
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["greeting"], "Hello, World!");

    shutdown.send(()).ok();
}

#[tokio::test]
async fn not_found() {
    let router = Router::new().get("/", || async { "home" });

    let (addr, shutdown) = start_server(router).await;

    let resp = http_get(addr, "/nonexistent").await;
    assert_eq!(resp.status(), 404);

    shutdown.send(()).ok();
}

#[tokio::test]
async fn method_not_allowed() {
    let router = Router::new().get("/only-get", || async { "ok" });

    let (addr, shutdown) = start_server(router).await;

    let resp = http_post_json(addr, "/only-get", "{}").await;
    assert_eq!(resp.status(), 405);

    shutdown.send(()).ok();
}

#[tokio::test]
async fn custom_fallback() {
    let router = Router::new()
        .get("/", || async { "home" })
        .fallback(|| async {
            (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": "custom 404" })),
            )
        });

    let (addr, shutdown) = start_server(router).await;

    let resp = http_get(addr, "/nope").await;
    assert_eq!(resp.status(), 404);

    let body = body_string(resp).await;
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["error"], "custom 404");

    shutdown.send(()).ok();
}

#[tokio::test]
async fn middleware_adds_headers() {
    let router = Router::new()
        .middleware(RequestId::new())
        .get("/", || async { "ok" });

    let (addr, shutdown) = start_server(router).await;

    let resp = http_get(addr, "/").await;
    assert_eq!(resp.status(), 200);
    assert!(resp.headers().contains_key("x-request-id"));

    shutdown.send(()).ok();
}

#[tokio::test]
async fn cors_preflight() {
    let router = Router::new()
        .middleware(
            Cors::new()
                .allow_any_origin()
                .allow_any_method()
                .allow_any_header(),
        )
        .get("/api", || async { "data" });

    let (addr, shutdown) = start_server(router).await;

    // Make an OPTIONS request
    let stream = TcpStream::connect(addr).await.unwrap();
    let io = TokioIo::new(stream);
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await.unwrap();
    tokio::spawn(conn);

    let req = hyper::Request::builder()
        .method("OPTIONS")
        .uri("/api")
        .header("host", format!("127.0.0.1:{}", addr.port()))
        .header("origin", "https://example.com")
        .header("access-control-request-method", "GET")
        .body(Full::new(Bytes::new()))
        .unwrap();

    let resp = sender.send_request(req).await.unwrap();
    // CORS should respond to preflight
    assert!(resp.headers().contains_key("access-control-allow-origin"));

    shutdown.send(()).ok();
}

#[tokio::test]
async fn multiple_requests_on_different_routes() {
    let router = Router::new()
        .get("/a", || async { "route A" })
        .get("/b", || async { "route B" })
        .post("/c", |Json(v): Json<serde_json::Value>| async move {
            Json(serde_json::json!({ "echo": v }))
        });

    let (addr, shutdown) = start_server(router).await;

    let resp = http_get(addr, "/a").await;
    assert_eq!(resp.status(), 200);
    assert_eq!(body_string(resp).await, "route A");

    let resp = http_get(addr, "/b").await;
    assert_eq!(resp.status(), 200);
    assert_eq!(body_string(resp).await, "route B");

    let resp = http_post_json(addr, "/c", r#"{"hello":"world"}"#).await;
    assert_eq!(resp.status(), 200);
    let body = body_string(resp).await;
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["echo"]["hello"], "world");

    shutdown.send(()).ok();
}

#[tokio::test]
async fn nested_router() {
    let api = Router::new()
        .get("/items", || async { Json(serde_json::json!(["a", "b"])) })
        .get("/items/:id", |Path(id): Path<u64>| async move {
            Json(serde_json::json!({ "id": id }))
        });

    let router = Router::new()
        .get("/", || async { "home" })
        .nest("/api", api);

    let (addr, shutdown) = start_server(router).await;

    let resp = http_get(addr, "/api/items").await;
    assert_eq!(resp.status(), 200);
    let body = body_string(resp).await;
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json, serde_json::json!(["a", "b"]));

    let resp = http_get(addr, "/api/items/7").await;
    assert_eq!(resp.status(), 200);
    let body = body_string(resp).await;
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["id"], 7);

    shutdown.send(()).ok();
}

#[tokio::test]
async fn state_sharing() {
    use std::sync::atomic::{AtomicU64, Ordering};

    let counter = Arc::new(AtomicU64::new(0));

    let router =
        Router::new()
            .state(counter)
            .get("/count", |State(c): State<Arc<AtomicU64>>| async move {
                let n = c.fetch_add(1, Ordering::Relaxed);
                Json(serde_json::json!({ "count": n }))
            });

    let (addr, shutdown) = start_server(router).await;

    let resp = http_get(addr, "/count").await;
    let body = body_string(resp).await;
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["count"], 0);

    let resp = http_get(addr, "/count").await;
    let body = body_string(resp).await;
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["count"], 1);

    shutdown.send(()).ok();
}

#[tokio::test]
async fn graceful_shutdown() {
    let router = Router::new().get("/", || async { "alive" });

    let (addr, shutdown) = start_server(router).await;

    // Server is running
    let resp = http_get(addr, "/").await;
    assert_eq!(resp.status(), 200);

    // Trigger shutdown
    shutdown.send(()).ok();

    // Give it a moment to shut down
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Server should no longer accept connections
    let result = TcpStream::connect(addr).await;
    assert!(
        result.is_err(),
        "Server should have stopped accepting connections"
    );
}

#[tokio::test]
async fn query_params() {
    #[derive(Deserialize)]
    struct Params {
        page: u32,
        limit: u32,
    }

    let router = Router::new().get("/items", |Query(p): Query<Params>| async move {
        Json(serde_json::json!({ "page": p.page, "limit": p.limit }))
    });

    let (addr, shutdown) = start_server(router).await;

    let resp = http_get(addr, "/items?page=3&limit=25").await;
    assert_eq!(resp.status(), 200);

    let body = body_string(resp).await;
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["page"], 3);
    assert_eq!(json["limit"], 25);

    shutdown.send(()).ok();
}

#[tokio::test]
async fn status_code_tuple() {
    let router = Router::new().post("/created", || async {
        (StatusCode::CREATED, Json(serde_json::json!({ "id": 1 })))
    });

    let (addr, shutdown) = start_server(router).await;

    let resp = http_post_json(addr, "/created", "{}").await;
    assert_eq!(resp.status(), 201);

    shutdown.send(()).ok();
}

#[tokio::test]
async fn global_body_limit_rejects_large_content_length() {
    let router = Router::new().post("/upload", |body: String| async move { body });

    // Set a 100-byte global body limit
    let (addr, shutdown) = start_server_with_body_limit(router, 100).await;

    // Send a request with Content-Length larger than the limit
    let stream = TcpStream::connect(addr).await.unwrap();
    let io = TokioIo::new(stream);
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await.unwrap();
    tokio::spawn(conn);

    let req = hyper::Request::builder()
        .method("POST")
        .uri("/upload")
        .header("host", format!("127.0.0.1:{}", addr.port()))
        .header("content-length", "200")
        .body(Full::new(Bytes::from("x".repeat(200))))
        .unwrap();

    let resp = sender.send_request(req).await.unwrap();
    assert_eq!(resp.status(), 413);

    shutdown.send(()).ok();
}

#[tokio::test]
async fn global_body_limit_allows_small_body() {
    let router = Router::new().post("/upload", |body: String| async move { body });

    let (addr, shutdown) = start_server_with_body_limit(router, 100).await;

    let resp = http_post_json(addr, "/upload", r#""hello""#).await;
    assert_eq!(resp.status(), 200);

    shutdown.send(()).ok();
}

#[tokio::test]
async fn head_request_returns_headers_no_body() {
    let router = Router::new().get("/data", || async { "hello world" });

    let (addr, shutdown) = start_server(router).await;

    // Send HEAD request
    let stream = TcpStream::connect(addr).await.unwrap();
    let io = TokioIo::new(stream);
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await.unwrap();
    tokio::spawn(conn);

    let req = hyper::Request::builder()
        .method("HEAD")
        .uri("/data")
        .header("host", format!("127.0.0.1:{}", addr.port()))
        .body(Full::new(Bytes::new()))
        .unwrap();

    let resp = sender.send_request(req).await.unwrap();
    assert_eq!(resp.status(), 200);

    // Body should be empty for HEAD requests
    let body = body_string(resp).await;
    assert!(body.is_empty(), "HEAD response should have empty body");

    shutdown.send(()).ok();
}

// ---------------------------------------------------------------------------
// RS-05 regressions: configured body ceilings survive streaming/chunked
// transports.
// ---------------------------------------------------------------------------

/// A chunked body (no Content-Length) larger than a lowered max_body_size
/// must be rejected 413 — the old code only checked the Content-Length
/// header, so chunked uploads bypassed the configured ceiling entirely.
#[tokio::test]
async fn chunked_body_over_configured_limit_is_413() {
    use http_body_util::StreamBody;

    let router = Router::new().post("/upload", |body: String| async move {
        format!("got {} bytes", body.len())
    });
    let (addr, shutdown) = start_server_with_body_limit(router, 8).await;

    let io = TokioIo::new(TcpStream::connect(addr).await.unwrap());
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await.unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });

    // 16 bytes in 4-byte frames, no total length known up front → hyper
    // sends Transfer-Encoding: chunked.
    let stream = async_stream::stream! {
        for chunk in b"1234567890abcdef".chunks(4) {
            yield Ok::<_, std::convert::Infallible>(http_body::Frame::data(Bytes::copy_from_slice(chunk)));
        }
    };
    let req = hyper::Request::builder()
        .method("POST")
        .uri("/upload")
        .body(StreamBody::new(stream))
        .unwrap();
    let resp = sender.send_request(req).await.unwrap();
    assert_eq!(resp.status(), 413, "chunked body over the limit must 413");

    shutdown.send(()).ok();
}

/// A chunked body WITHIN the configured limit passes through and is readable
/// by extractors (no over-eager truncation).
#[tokio::test]
async fn chunked_body_within_limit_passes() {
    use http_body_util::StreamBody;

    let router = Router::new().post("/upload", |body: String| async move {
        format!("len={}", body.len())
    });
    let (addr, shutdown) = start_server_with_body_limit(router, 64).await;

    let io = TokioIo::new(TcpStream::connect(addr).await.unwrap());
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await.unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let stream = async_stream::stream! {
        for chunk in b"1234567890abcdef".chunks(4) {
            yield Ok::<_, std::convert::Infallible>(http_body::Frame::data(Bytes::copy_from_slice(chunk)));
        }
    };
    let req = hyper::Request::builder()
        .method("POST")
        .uri("/upload")
        .body(StreamBody::new(stream))
        .unwrap();
    let resp = sender.send_request(req).await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&body[..], b"len=16");

    shutdown.send(()).ok();
}

// RS-06/07: run on a current-thread runtime, including the macOS shared-listener path.
#[tokio::test]
async fn listener_modes_drain_before_reverse_shutdown_hooks() {
    for workers in [0, 2] {
        let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = reservation.local_addr().unwrap();
        drop(reservation);
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let order = Arc::new(std::sync::Mutex::new(Vec::new()));
        let entered_handler = entered.clone();
        let release_handler = release.clone();
        let router = Router::new().get("/held", move || {
            let entered = entered_handler.clone();
            let release = release_handler.clone();
            async move {
                entered.notify_one();
                release.notified().await;
                "finished"
            }
        });
        let (stop, stopped) = oneshot::channel();
        let first = order.clone();
        let second = order.clone();
        let server = tokio::spawn(async move {
            Neutron::new()
                .router(router)
                .workers(workers)
                .shutdown_timeout(Duration::from_secs(2))
                .shutdown_signal(async move {
                    let _ = stopped.await;
                })
                .on_shutdown(move || {
                    let order = first.clone();
                    async move {
                        order.lock().unwrap().push(1);
                    }
                })
                .on_shutdown(move || {
                    let order = second.clone();
                    async move {
                        order.lock().unwrap().push(2);
                    }
                })
                .listen(addr)
                .await
                .unwrap();
        });
        tokio::time::sleep(Duration::from_millis(60)).await;
        let request = tokio::spawn(http_get(addr, "/held"));
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        stop.send(()).unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(
            order.lock().unwrap().is_empty(),
            "hooks closed dependencies before drain, workers={workers}"
        );
        release.notify_one();
        let response = request.await.unwrap();
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            "finished"
        );
        tokio::time::timeout(Duration::from_secs(3), server)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(*order.lock().unwrap(), vec![2, 1]);
        assert!(TcpStream::connect(addr).await.is_err());
    }
}

#[tokio::test]
async fn drain_deadline_aborts_and_joins_owned_handlers() {
    struct Dropped(Arc<std::sync::atomic::AtomicBool>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }
    for workers in [0, 2] {
        let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = reservation.local_addr().unwrap();
        drop(reservation);
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let entered = Arc::new(tokio::sync::Notify::new());
        let entered_handler = entered.clone();
        let dropped_handler = dropped.clone();
        let router = Router::new().get("/held", move || {
            let dropped = Dropped(dropped_handler.clone());
            let entered = entered_handler.clone();
            async move {
                let _guard = dropped;
                entered.notify_one();
                std::future::pending::<()>().await;
                "unreachable"
            }
        });
        let (stop, stopped) = oneshot::channel();
        let server = tokio::spawn(async move {
            Neutron::new()
                .router(router)
                .workers(workers)
                .shutdown_timeout(Duration::from_millis(50))
                .shutdown_signal(async move {
                    let _ = stopped.await;
                })
                .listen(addr)
                .await
                .unwrap();
        });
        tokio::time::sleep(Duration::from_millis(60)).await;
        let request = tokio::spawn(http_get(addr, "/held"));
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap();
        assert!(
            dropped.load(std::sync::atomic::Ordering::SeqCst),
            "handler outlived serve, workers={workers}"
        );
        request.abort();
    }
}

#[cfg(feature = "tls")]
#[tokio::test]
async fn stalled_tls_handshake_does_not_outlive_shutdown() {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let tls = neutron::tls::TlsConfig::from_pem_bytes(
        cert.cert.pem().as_bytes(),
        cert.key_pair.serialize_pem().as_bytes(),
    )
    .unwrap();
    let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = reservation.local_addr().unwrap();
    drop(reservation);
    let (stop, stopped) = oneshot::channel();
    let server = tokio::spawn(async move {
        Neutron::new()
            .shutdown_timeout(Duration::from_millis(50))
            .shutdown_signal(async move {
                let _ = stopped.await;
            })
            .listen_tls(addr, tls)
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let _stalled = TcpStream::connect(addr).await.unwrap();
    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();
}

#[cfg(feature = "http3")]
#[tokio::test]
async fn h3_honors_shutdown_signal_and_hooks() {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let tls = neutron::tls::TlsConfig::from_pem_bytes(
        cert.cert.pem().as_bytes(),
        cert.key_pair.serialize_pem().as_bytes(),
    )
    .unwrap();
    let reservation = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let addr = reservation.local_addr().unwrap();
    drop(reservation);
    let (stop, stopped) = oneshot::channel();
    let order = Arc::new(std::sync::Mutex::new(Vec::new()));
    let first = order.clone();
    let second = order.clone();
    let server = tokio::spawn(async move {
        Neutron::new()
            .shutdown_timeout(Duration::from_millis(50))
            .on_shutdown(move || {
                let order = first.clone();
                async move {
                    order.lock().unwrap().push(1);
                }
            })
            .on_shutdown(move || {
                let order = second.clone();
                async move {
                    order.lock().unwrap().push(2);
                }
            })
            .shutdown_signal(async move {
                let _ = stopped.await;
            })
            .listen_h3(addr, tls)
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(*order.lock().unwrap(), vec![2, 1]);
}

#[cfg(feature = "ws")]
#[tokio::test]
async fn websocket_split_preserves_partial_frames_during_outbound_writes() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let send_now = Arc::new(tokio::sync::Notify::new());
    let received = Arc::new(tokio::sync::Notify::new());
    let outbound = send_now.clone();
    let inbound = received.clone();
    let router = Router::new().get("/ws", move |upgrade: neutron::ws::WebSocketUpgrade| {
        let outbound = outbound.clone();
        let inbound = inbound.clone();
        async move {
            upgrade.on_upgrade(move |socket| async move {
                let (sender, mut receiver) = socket.split();
                let writer = tokio::spawn(async move {
                    outbound.notified().await;
                    sender
                        .send(neutron::ws::Message::Text("outbound".into()))
                        .await
                        .unwrap();
                    sender
                });
                let message = receiver.recv().await.unwrap();
                assert_eq!(message.as_text().unwrap(), "hello");
                inbound.notify_one();
                let sender = writer.await.unwrap();
                sender
                    .send(neutron::ws::Message::Close(None))
                    .await
                    .unwrap();
            })
        }
    });
    let (addr, stop) = start_server(router).await;
    let mut peer = TcpStream::connect(addr).await.unwrap();
    peer.write_all(b"GET /ws HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n").await.unwrap();
    let mut headers = Vec::new();
    while !headers.ends_with(b"\r\n\r\n") {
        headers.push(peer.read_u8().await.unwrap());
    }
    // Send only frame header: the reader must retain it while the server writes.
    peer.write_all(&[0x81, 0x85]).await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    send_now.notify_one();
    let mut header = [0; 2];
    peer.read_exact(&mut header).await.unwrap();
    assert_eq!(header, [0x81, 8]);
    let mut bytes = [0; 8];
    peer.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"outbound");
    let mut tail = vec![1, 2, 3, 4];
    tail.extend(
        b"hello"
            .iter()
            .enumerate()
            .map(|(i, b)| b ^ [1, 2, 3, 4][i % 4]),
    );
    peer.write_all(&tail).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), received.notified())
        .await
        .unwrap();
    peer.read_exact(&mut header).await.unwrap();
    assert_eq!(header[0] & 15, 8);
    stop.send(()).unwrap();
}
