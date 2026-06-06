# Neutron Rust — Surgical Implementation Spec (P1.5b+P1.2, P1.6)

> Execution-ready, increment-by-increment. **`cargo test -p neutron` MUST be green after every numbered step.**
> Companion to `rust.md` (the plan). Where this spec diverges from `rust.md`'s sketch, the divergence is
> deliberate and flagged — the sketch was an outline; this is what actually compiles against the tree as it
> stands on `framework/rust-excellence` (HEAD `6eefe44`).
>
> **Branch reality:** the crate lives at `rust/crates/neutron/`. All paths below are repo-relative.
>
> **Verified current state (read, not assumed):**
> - `extract.rs:68-79` — traits are **sync**: `fn from_parts(req: &Request) -> Result<Self, Response>` /
>   `fn from_request(req: &Request) -> Result<Self, Response>`. Error type is already `Response` (not an
>   associated `Rejection` type). The `reject()` helper (`extract.rs:52-55`) already routes through
>   `AppError::from_status` → `application/problem+json` (P1.5a, done).
> - `handler.rs:198-208` — `Request` holds `body: Bytes`.
> - `handler.rs:613-635` — `impl_handler!` extracts **synchronously before** the async block, then moves
>   results into `Box::pin(async move { ... })`. Bound is `F: Fn(..) -> Fut + Send + Sync + 'static`
>   (**no `Clone`**).
> - `handler.rs:600-610` — zero-arg `Handler<()>` impl, separate from the macro.
> - `app.rs` body-collect sites: **`:319-335` (HTTP/1 plain), `:683-701` (also in `listen`, the same fn —
>   see note), `:1012-1030` (TLS)**. The plan cites `:313/:677/:1006`; the real first-line-of-block numbers
>   on this HEAD are `:319/:683/:1012`. There are **three** `Limited::new(body, body_limit).collect()` sites
>   in `app.rs` plus a **fourth** collect in `http3_server.rs:183-208` and **two** in `testing.rs`
>   (`:166`-area client, `:337` server). All six construct `Request::with_state(.., Bytes, ..)`.
> - `router.rs:794-822` — `RouterService::call` **also** collects (`body.collect().await`) at `:812-816`.
>   This is a **seventh** collect site introduced by P1.1; it must convert to pass-through too.
> - `router.rs:277` — `pub struct Router` has **no** generic param. No `FromRef` anywhere in the tree
>   (`grep -r FromRef rust/crates` = 0 hits).
> - Workspace `rust/Cargo.toml` members list has **no** `neutron-macros`; edition = 2021 (RPITIT/AFIT OK).

---

## PART A — P1.5b + P1.2 (async extractors + streaming): ONE coupled change

These two are coupled because (a) the trait must be async before any body extractor can `.await` a
streaming collect, and (b) once `Request.body` is `Body` instead of `Bytes`, the body extractors
**cannot** be sync — there is nothing to synchronously read. So we land the async trait first (with the
body still `Bytes`, all green), then flip the body to `Body` (extractors now `.await`).

### A.0 — Trait shape decision (the load-bearing signature)

`rust.md` sketches `type Rejection: IntoResponse; async fn from_request(req: Request) -> Result<Self,
Self::Rejection>`. **We do NOT take that shape.** Reasons, concrete to this tree:

1. The whole codebase's error channel is already `Response` (the `reject()` helper, every existing impl,
   every test via `err_or_panic(...) -> Response`). Introducing `type Rejection` re-types ~15 impls and
   ~40 tests for zero behavioral gain — rejections already render as problem+json. Keep `Err = Response`.
2. `async fn` in a public trait does **not** guarantee the returned future is `Send`. The desugaring of
   `async fn from_request(&self) -> T` is `fn from_request(&self) -> impl Future<Output = T>` with **no**
   `Send` bound. Our `Handler` requires `Pin<Box<dyn Future + Send>>`, so a non-`Send` extractor future
   would fail to box. We therefore write the **explicit RPITIT form with a `+ Send` bound** so the
   compiler enforces `Send` at the trait level (this is exactly what Axum 0.8 does to drop `async_trait`):

```rust
// extract.rs — the new trait pair (edition 2021, stable RPITIT since 1.75)
pub trait FromRequestParts: Sized + Send {
    fn from_parts(
        req: &Request,
    ) -> impl std::future::Future<Output = Result<Self, Response>> + Send;
}

pub trait FromRequest: Sized + Send {
    // `&mut Request` — body extractors need to mutate (take/stream) the body in A.7.
    fn from_request(
        req: &mut Request,
    ) -> impl std::future::Future<Output = Result<Self, Response>> + Send;
}
```

> Why `+ Send` and not `async fn`: `async fn from_parts(req: &Request) -> Result<Self, Response>` is sugar
> for `-> impl Future<Output = Result<Self, Response>>` **without** `Send`. The handler boxes these futures
> into `dyn Future + Send`; without the bound, any impl that holds a non-`Send` value across an `.await`
> (or even just the auto-trait leakage of a captured non-`Send` temporary) makes the handler impl
> uncallable, and the error surfaces deep in `impl_handler!` instead of at the extractor. The explicit
> `+ Send` makes the bound a trait obligation checked at each `impl` site — clean errors, guaranteed
> boxability. This is the "Send-bound desugaring" the task asks for.

> Why `&Request` for parts but `&mut Request` for `FromRequest`: parts extractors (Path/Query/State/headers)
> only read. Body extractors must consume/stream the body, which in A.6 becomes an owned `Body` behind the
> `&mut`. Keeping parts on `&Request` means the blanket impl (below) and all ~12 parts impls stay
> immutable-borrow and need only the `async` wrapper, not signature churn.

Blanket impl (unchanged intent, new async body):
```rust
impl<T: FromRequestParts> FromRequest for T {
    async fn from_request(req: &mut Request) -> Result<Self, Response> {
        // parts extractors don't touch the body; immutable reborrow is fine.
        T::from_parts(req).await
    }
}
```
> Note: writing `async fn` in the **impl** is allowed and ergonomic; the `+ Send` obligation from the
> trait is what's checked. The impl body's future is `Send` as long as `T::from_parts`'s is.

### A.1 — Make the trait async, keep `Request.body: Bytes` (NO behavior change)

**Goal:** flip the trait to RPITIT, wrap every existing impl body in `async`, fix the macro, fix all
call sites/tests — while the body is *still* `Bytes` so nothing actually awaits I/O yet. This isolates the
"async-ify the trait" diff from the "stream the body" diff. After this step the framework behaves
identically; only the types moved.

**A.1.1 — `extract.rs` traits.** Replace `extract.rs:68-91` (the two trait decls + blanket impl) with the
three blocks from A.0. The `reject()` helper stays as-is.

**A.1.2 — Parts impls (~12).** Each becomes `async`. Representative diffs:

```rust
// State<T> — extract.rs:207
impl<T: Clone + Send + Sync + 'static> FromRequestParts for State<T> {
    async fn from_parts(req: &Request) -> Result<Self, Response> {       // + async
        req.get_state::<T>().cloned().map(State).ok_or_else(|| {
            tracing::error!("State<{}> not found — did you call Router::state()?",
                std::any::type_name::<T>());
            reject(StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error")
        })
    }
}

// Path<T> — extract.rs:163
impl<T: PathParam + Send + 'static> FromRequestParts for Path<T> {
    async fn from_parts(req: &Request) -> Result<Self, Response> {       // + async
        T::from_params(req.params()).map(Path)
            .map_err(|msg| reject(StatusCode::BAD_REQUEST, msg))
    }
}
```
Apply the mechanical `+ async` to: `Query` (`:182`), `Method` (`:226`), `Uri` (`:232`), `HeaderMap`
(`:238`), `ConnectInfo` (`:257`), `Extension` (`:284`), `Optional` (`:419`), `TypedHeader` (`:473`).
`Optional` additionally must `.await` its inner: `match T::from_parts(req).await { ... }`.

**A.1.3 — Body impls (4) — still read `req.body()` (a `Bytes`) in A.1.** They move from `FromRequest`
(sync) to `FromRequest` (async) but keep reading the buffered `Bytes`. Representative body extractor diff
(this is the one the task asks to show):

```rust
// Bytes — extract.rs:304  (signature is now &mut Request)
impl FromRequest for Bytes {
    async fn from_request(req: &mut Request) -> Result<Self, Response> {
        Ok(req.body().clone())          // A.1: still a buffered Bytes; A.7 swaps this body
    }
}

// Json<T> — extract.rs:323
#[cfg(feature = "json")]
impl<T: DeserializeOwned + Send + 'static> FromRequest for Json<T> {
    async fn from_request(req: &mut Request) -> Result<Self, Response> {
        let content_type = req.headers().get(http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()).unwrap_or("");
        if !content_type.starts_with("application/json") {
            return Err(reject(StatusCode::UNSUPPORTED_MEDIA_TYPE, "Expected application/json"));
        }
        json_from_slice(req.body())     // A.1: &Bytes; A.7: collected bytes
            .map(Json)
            .map_err(|e| reject(StatusCode::BAD_REQUEST, format!("Invalid JSON: {e}")))
    }
}
```
Same for `String` (`:311`) and `Form<T>` (`:376`): add `async`, change to `&mut Request`, body read
unchanged.

**A.1.4 — Fix `impl_handler!` (`handler.rs:613-635`).** This is the macro rewrite the task asks for:
add `F: Clone`, clone the handler into the async block, and **extract + await INSIDE the block**.

```rust
macro_rules! impl_handler {
    ($($T:ident),+) => {
        #[allow(non_snake_case)]
        impl<F, Fut, Res, $($T,)+> Handler<($($T,)+)> for F
        where
            F: Fn($($T,)+) -> Fut + Clone + Send + Sync + 'static,   // + Clone
            Fut: Future<Output = Res> + Send + 'static,
            Res: IntoResponse,
            $($T: FromRequest + 'static,)+
        {
            fn call(&self, req: Request) -> Pin<Box<dyn Future<Output = Response> + Send>> {
                let this = self.clone();          // clone handler -> owned, moved into block
                Box::pin(async move {
                    let mut req = req;            // own it; body extractors need &mut
                    $(
                        // extract + AWAIT inside the async block, sequentially.
                        let $T = match $T::from_request(&mut req).await {
                            Ok(v) => v,
                            Err(e) => return e,   // already a Response
                        };
                    )+
                    (this)($($T,)+).await.into_response()
                })
            }
        }
    };
}
```
Key changes vs the old body:
- The old code extracted synchronously **before** `Box::pin` and could `return Box::pin(async move { e })`
  on error. Now extraction is inside the future; on error we just `return e` (a `Response`) — same effect,
  one fewer box.
- `let this = self.clone()` is required because `&self` cannot be borrowed across the `'static` future; we
  need an owned handler. Hence `F: Clone`. **Every fn item and every closure that is `Copy`/`Clone` already
  satisfies this** — `fn`-pointers are `Copy`; non-capturing closures are `Copy`; capturing closures are
  `Clone` if their captures are. The zero-arg impl (`handler.rs:600-610`) must get the **same** `F: Clone`
  treatment for consistency:
```rust
impl<F, Fut, Res> Handler<()> for F
where
    F: Fn() -> Fut + Clone + Send + Sync + 'static,   // + Clone
    Fut: Future<Output = Res> + Send + 'static,
    Res: IntoResponse,
{
    fn call(&self, _req: Request) -> Pin<Box<dyn Future<Output = Response> + Send>> {
        let this = self.clone();
        Box::pin(async move { (this)().await.into_response() })
    }
}
```

**A.1.5 — Fix every direct extractor call in tests.** All `extract.rs` tests call `X::from_parts(&req)` /
`X::from_request(&req)` synchronously via `ok_or_panic`/`err_or_panic`. They must now `.await`. Two
mechanical edits:
- Change `#[test]` → `#[tokio::test]` and `async` on every test that calls an extractor directly
  (the `path_param_*` pure-`PathParam` tests do **not** call extractors — leave them sync).
- `ok_or_panic(Path::<u64>::from_parts(&req))` → `ok_or_panic(Path::<u64>::from_parts(&req).await)`.
- Body extractor tests: `Bytes::from_request(&req)` → needs `&mut req`:
  `let mut req = ...; ok_or_panic(Bytes::from_request(&mut req).await)`.
- The `from_request_parts_blanket_impl` test (`:1313`) → `<Method as FromRequest>::from_request(&mut req).await`.

**Test to ADD (proves Send-ness compiles):**
```rust
// extract.rs tests
#[test]
fn extractor_futures_are_send() {
    fn assert_send<F: Send>(_: F) {}
    let req = Request::new(Method::GET, "/".parse().unwrap(), HeaderMap::new(), Bytes::new());
    // If any extractor's future were !Send this line would not compile.
    assert_send(async move { let mut r = req; let _ = String::from_request(&mut r).await; });
}
```

**CHECKPOINT A.1:** `cargo test -p neutron` green. (Behavior identical; only the trait went async.)
Run `cargo test -p neutron --no-default-features` and `--all-features` too — the macro change is
feature-independent and a feature-matrix break here is cheap to catch now.

### A.2 — `Request` gains a `Body` field alongside `Bytes` (transitional)

**Do not** rip out `Bytes` yet. Add the streaming machinery in parallel so the flip in A.6 is atomic.

**A.2.1 — `Body` already exists** (`handler.rs:41-100`) as a response body (`Full | Stream`, error
`Infallible`). For **request** bodies the error type is **not** `Infallible` (hyper's `Incoming` can error
mid-stream). Add a dedicated request-body alias to avoid overloading the response `Body`:
```rust
// handler.rs — request body is a boxed http_body::Body with a real error.
pub type ReqBody = Pin<Box<
    dyn HttpBody<Data = Bytes, Error = Box<dyn std::error::Error + Send + Sync>> + Send
>>;
```
> Rationale: the response `Body` is `Error = Infallible` and that invariant is relied on across
> `IntoResponse`. Request bodies can fail (connection reset mid-upload), so they need a fallible error.
> A separate `ReqBody` keeps both invariants honest.

**A.2.2 — Add the field + accessor + collector to `Request` (`handler.rs:198`).** Keep `body: Bytes` for
now; add:
```rust
pub struct Request {
    // ... existing fields ...
    body: Bytes,                 // REMOVED in A.6
    body_stream: Option<ReqBody>,// ADDED now; Some(..) once A.6 lands
    // ...
}

impl Request {
    /// Collect the request body with a hard byte ceiling, enforced *during* streaming.
    /// Returns 413 (as a `Response`) the instant the running total exceeds `limit`.
    pub async fn collect_body(&mut self, limit: usize) -> Result<Bytes, Response> {
        // A.2: body_stream is always None -> fall back to the buffered Bytes.
        if let Some(stream) = self.body_stream.take() {
            collect_limited(stream, limit).await
        } else {
            Ok(self.body.clone())   // transitional path; removed in A.6
        }
    }

    /// Take the streaming body for frame-level consumption (BodyStream extractor).
    pub fn take_body(&mut self) -> Option<ReqBody> { self.body_stream.take() }
}
```
And the free fn (new, `handler.rs`):
```rust
async fn collect_limited(mut body: ReqBody, limit: usize) -> Result<Bytes, Response> {
    use http_body_util::BodyExt;
    let mut acc = bytes::BytesMut::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| AppError::bad_request(
            "The request body could not be read.").into_response())?;
        if let Ok(data) = frame.into_data() {
            if acc.len() + data.len() > limit {
                return Err(AppError::payload_too_large(
                    "The request body exceeds the configured limit.").into_response());
            }
            acc.extend_from_slice(&data);
        }
    }
    Ok(acc.freeze())
}
```
> This is the **streaming 413**: the limit is checked per-frame as bytes arrive, so an attacker streaming a
> 10 GB chunked body is rejected after `limit+1` bytes, never buffered whole. Replaces the pre-collect
> `Limited::new` cap.

**A.2.3 — `Request::new` / `with_state` set `body_stream: None`.** Both constructors (`:211`, `:228`) add
`body_stream: None,` to the struct literal. (Synthetic/test requests stay buffered.)

**CHECKPOINT A.2:** `cargo test -p neutron` green. `collect_body` exists but every caller still gets the
buffered `Bytes` path. Add a unit test:
```rust
#[tokio::test]
async fn collect_body_transitional_returns_buffered() {
    let mut req = Request::new(Method::POST, "/".parse().unwrap(), HeaderMap::new(),
        Bytes::from("hello"));
    assert_eq!(req.collect_body(1024).await.unwrap(), Bytes::from("hello"));
}
```

### A.3 — Body extractors switch to `collect_body` (still buffered underneath)

Change the 4 body extractors to call `req.collect_body(limit).await?` instead of reading `req.body()`.
The limit needs to reach the extractor — for now use the global default constant; the real wiring (per the
plan's per-route override, P2.8) is out of scope here. Expose the global from `app.rs`:
```rust
// app.rs: make the constant visible to extract.rs
pub(crate) const DEFAULT_MAX_BODY_SIZE: usize = 2 * 1024 * 1024;
```
```rust
// extract.rs — Json now collects (A.3); body source is collect_body, still buffered in A.3.
#[cfg(feature = "json")]
impl<T: DeserializeOwned + Send + 'static> FromRequest for Json<T> {
    async fn from_request(req: &mut Request) -> Result<Self, Response> {
        let ct = req.headers().get(http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()).unwrap_or("").to_owned();
        if !ct.starts_with("application/json") {
            return Err(reject(StatusCode::UNSUPPORTED_MEDIA_TYPE, "Expected application/json"));
        }
        let bytes = req.collect_body(crate::app::DEFAULT_MAX_BODY_SIZE).await?;  // <-- await
        json_from_slice(&bytes).map(Json)
            .map_err(|e| reject(StatusCode::BAD_REQUEST, format!("Invalid JSON: {e}")))
    }
}
```
> Note the `.to_owned()` on the content-type: in A.6 the body lives behind `&mut req`, and holding a
> `&str` borrow of `req.headers()` across the `&mut req.collect_body()` call would be a borrow conflict.
> Copy the small content-type string out first. Do the same in `Form`.

`Bytes`/`String` similarly call `collect_body`. **CHECKPOINT A.3:** `cargo test -p neutron` green
(buffered path still). All existing body-extractor tests pass unchanged because the synthetic requests
carry buffered `Bytes`.

### A.4 — `BodyStream` extractor (frame-level, backpressured)

New extractor, the plan's "body extractor is last" mechanism (enforced naturally — only one `FromRequest`
that consumes the body can run; a second body extractor gets an empty stream):
```rust
// extract.rs
pub struct BodyStream(pub crate::handler::ReqBody);
impl FromRequest for BodyStream {
    async fn from_request(req: &mut Request) -> Result<Self, Response> {
        match req.take_body() {
            Some(b) => Ok(BodyStream(b)),
            None => Ok(BodyStream(empty_req_body())),  // already consumed -> empty
        }
    }
}
```
where `empty_req_body()` boxes `http_body_util::Empty<Bytes>` mapped to the `ReqBody` error. **CHECKPOINT
A.4:** green; add `body_stream_extractor_yields_stream` once A.6 provides a real stream — for now assert it
yields an empty stream.

### A.5 — Convert the SEVEN collect sites to pass-through `ReqBody` (the dispatch surgery)

This is the step that actually stops pre-buffering. Each site currently does
`Limited::new(body, body_limit).collect().await` → `Bytes` → `with_state(.., bytes, ..)`. Replace with:
(1) keep the cheap `Content-Length > limit` early-413; (2) box the hyper body into `ReqBody`; (3) set it
on the request via a new `with_streaming_state` constructor (or `set_body_stream`).

Add the constructor (`handler.rs`):
```rust
pub(crate) fn with_streaming_state(
    method: Method, uri: Uri, headers: HeaderMap, body: ReqBody, state: Arc<StateMap>,
) -> Self {
    Self { method, uri, headers, body: Bytes::new(), body_stream: Some(body),
           params: SmallVec::new(), state, on_upgrade: Mutex::new(None),
           extensions: SmallVec::new(), remote_addr: None }
}
```

**Site 1 — `app.rs:319-343` (HTTP/1 in `listen`).** Replace the `body_bytes = if request_has_body {..}`
block + `with_state(.., body_bytes, ..)` with:
```rust
// keep the Content-Length early reject above this (unchanged)
let (parts, body) = req.into_parts();
let boxed: ReqBody = Box::new(body.map_err(|e|
    Box::new(e) as Box<dyn std::error::Error + Send + Sync>));
let mut neutron_req = NeutronRequest::with_streaming_state(
    parts.method, parts.uri, parts.headers, Box::pin(boxed), state);
```
> `hyper::body::Incoming: HttpBody<Data=Bytes, Error=hyper::Error>`. `.map_err(..)` adapts to the
> `ReqBody` error type. `request_has_body` fast path is no longer needed for *skipping collection*
> (collection is now lazy), but keep the bodyless short-circuit only if a measured win; default: drop it,
> the stream is empty for bodyless requests anyway.

**Site 2 — the second `service_fn` in `listen`** (the audit's `:677`/this HEAD `:683-707`). Identical
replacement. (Note: `listen` contains the accept loop's `service_fn`; `worker_accept_loop` is a *separate*
fn at `:301-352` with its own collect at `:319-335` — that's **Site 1**. The `:683` block is inside
`listen`'s inline `service_fn`. Both are real; both convert.)

**Site 3 — `listen_tls` `:1012-1030`.** Identical replacement.

**Site 4 — `RouterService::call` `router.rs:809-820`.** This collects `body.collect().await`. Convert:
```rust
fn call(&mut self, req: http::Request<Body>) -> Self::Future {
    let dispatch = Arc::clone(&self.dispatch);
    let state = Arc::clone(&self.state);
    Box::pin(async move {
        let (parts, body) = req.into_parts();
        let boxed: ReqBody = Box::pin(body.map_err(|e: std::convert::Infallible|
            match e {}));   // Body's error is Infallible -> never; adapt type
        let neutron_req = Request::with_streaming_state(
            parts.method, parts.uri, parts.headers, boxed, state);
        Ok(dispatch(neutron_req).await)
    })
}
```
> The response `Body` (Infallible) boxes into `ReqBody` (boxed error) via the never-type `match e {}`.

**Site 5 — `http3_server.rs:183-216`.** The h3 path manually loops `recv_data` into a `Vec<u8>` with an
inline limit check (`:194`). Replace the eager `Vec` accumulation with a `ReqBody` adapter over the
`RequestStream`, OR (pragmatic, lower-risk for h3's chunk API) keep the loop but feed it into a
`futures`-channel-backed `StreamBody` so the handler streams. **Minimum viable:** wrap the existing
collected `Vec` as a single-frame `Full`-style `ReqBody` and call `with_streaming_state` — this reaches the
HTTP/3 path with the new type **without** rewriting h3's recv loop, satisfying "reach the HTTP/3 path"
while deferring true h3 frame streaming to P1.9. Document the deferral inline.

**Sites 6 & 7 — `testing.rs`** (`TestClient` `:166`, `TestServer` `:337-349`). `TestClient` builds from a
`Bytes` body field → use `with_streaming_state` with a `Full`-boxed single frame, or keep `with_state`
(buffered) since tests want determinism. **Decision:** keep `TestClient`/`TestServer` on the buffered
`with_state` path (synthetic, no streaming needed) — they exercise extractors through `collect_body`'s
buffered branch, which after A.6 must still work for `Some(Full-frame)` streams. To avoid the A.6 removal
of the `Bytes` field breaking them, route them through `with_streaming_state(.., full_frame(self.body))`.

**Grep-enforced acceptance test (the plan's `no_body_collect_in_any_dispatch_path`):**
```rust
// tests/no_pre_collect.rs  (integration test in the crate)
#[test]
fn no_unconditional_body_collect_in_dispatch() {
    let app = include_str!("../src/app.rs");
    assert!(!app.contains("Limited::new"),
        "app.rs must not pre-collect request bodies (P1.2)");
    let router = include_str!("../src/router.rs");
    assert!(!router.contains("body\n                .collect()") &&
            !router.contains(".collect()\n                .await\n                .map(|c| c.to_bytes())"),
        "RouterService must pass Body through, not collect it");
}
```

**CHECKPOINT A.5:** `cargo test -p neutron` green; `--features http3` builds; `--all-features` green.

### A.6 — Remove `body: Bytes` from `Request`; `collect_body` is now always streaming

Delete the `body: Bytes` field and the `body()` accessor (or keep `body()` returning `&[]`-equivalent and
deprecate). `collect_body` loses its transitional `else` branch:
```rust
pub async fn collect_body(&mut self, limit: usize) -> Result<Bytes, Response> {
    match self.body_stream.take() {
        Some(stream) => collect_limited(stream, limit).await,
        None => Ok(Bytes::new()),   // already consumed or bodyless
    }
}
```
Fix the fallout: `Request::new` (public, used by tests) must still accept a `Bytes` and wrap it as a
single-frame stream:
```rust
pub fn new(method: Method, uri: Uri, headers: HeaderMap, body: Bytes) -> Self {
    Self { method, uri, headers, body_stream: Some(full_frame(body)), /* ...no body field... */ }
}
```
where `full_frame(b: Bytes) -> ReqBody` boxes `http_body_util::Full::new(b)` with its `Infallible` error
mapped via `match`. This keeps the **entire existing test suite source-compatible** (`Request::new(.., Bytes::new())`
still compiles) — the body just lives in `body_stream` now.

**CHECKPOINT A.6:** `cargo test -p neutron` green. Now `large_upload_streams_without_buffering` and
`body_limit_enforced_during_stream` are real:
```rust
#[tokio::test]
async fn body_limit_enforced_during_stream() {
    // Build a Request whose body_stream yields frames exceeding the limit; assert 413
    // without buffering the whole body (use a stream that panics if fully drained).
}
#[tokio::test]
async fn json_extractor_still_works_over_stream() {
    let mut req = Request::new(Method::POST, "/".parse().unwrap(),
        json_ct_headers(), Bytes::from(r#"{"name":"Alice","age":30}"#));
    let Json(u) = ok_or_panic(Json::<User>::from_request(&mut req).await);
    assert_eq!(u.name, "Alice");
}
```

### A.7 — h3 + protocol-parity verification

Re-run the keystone dispatch tests against HTTP/1, /2 (via `TestServer`, which uses real hyper) and h3
(`--features http3`). The acceptance gate from `rust.md`: "verified on HTTP/1, /2, /3." Add
`h3_streaming_upload` behind `#[cfg(feature = "http3")] #[ignore]` (needs a QUIC client harness; mark
ignored so default `cargo test` stays hermetic).

**FINAL CHECKPOINT (Part A):** `cargo test -p neutron`, `--no-default-features`, `--features json`,
`--features http3`, `--all-features` all green; `cargo clippy -p neutron --all-features -- -D warnings`
clean.

---

## PART B — P1.6: `Router<S>` + `FromRef` + `#[derive(FromRef)]`

> The single most invasive change. Land it as its own PR (the plan's residual-risk #1). Do **not** bundle
> with Part A.

### B.0 — PREREQUISITE: create the `neutron-macros` proc-macro crate FIRST

This must exist and build before any derive test. Mechanical setup:

1. `rust/crates/neutron-macros/Cargo.toml`:
```toml
[package]
name = "neutron-macros"
version.workspace = true
edition.workspace = true
license.workspace = true
[lib]
proc-macro = true
[dependencies]
syn = { version = "2", features = ["full"] }
quote = "1"
proc-macro2 = "1"
[dev-dependencies]
trybuild = "1"
```
2. Add `"crates/neutron-macros"` to `rust/Cargo.toml` `members`.
3. `neutron/Cargo.toml`: add `neutron-macros = { path = "../neutron-macros" }` and re-export in
   `neutron/src/lib.rs`: `pub use neutron_macros::FromRef;`.

**CHECKPOINT B.0:** `cargo build -p neutron-macros` succeeds with an empty `lib.rs`; `cargo test -p neutron`
still green (nothing references the macro yet).

### B.1 — `FromRef` trait (no router changes yet)

New file `neutron/src/from_ref.rs`:
```rust
/// Extract a sub-state `Self` from a borrowed app state `S`.
pub trait FromRef<S> { fn from_ref(state: &S) -> Self; }

/// Identity: any state is `FromRef` of itself.
impl<S: Clone> FromRef<S> for S { fn from_ref(s: &S) -> S { s.clone() } }
```
Export from `lib.rs`: `pub use from_ref::FromRef;`. **CHECKPOINT B.1:** green; add
`from_ref_identity_clones`.

### B.2 — `#[derive(FromRef)]`

Implement in `neutron-macros/src/lib.rs`. For `#[derive(FromRef)] struct AppState { db: Db, cache: Cache }`
generate one impl per field:
```rust
impl ::neutron::FromRef<AppState> for Db    { fn from_ref(s: &AppState) -> Db    { s.db.clone() } }
impl ::neutron::FromRef<AppState> for Cache { fn from_ref(s: &AppState) -> Cache { s.cache.clone() } }
```
Coherence guard: skip fields whose type equals the struct itself (the identity impl already covers `S:S`);
emit a `compile_error!` on tuple/enum/generic targets (out of scope). **CHECKPOINT B.2:** add a normal
unit test `derive_from_ref_extracts_substate` that constructs `AppState`, calls
`<Db as FromRef<AppState>>::from_ref(&s)`, asserts equality.

### B.3 — `Router<S = ()>` — thread the generic (the surgery)

This re-types the router surface. Exact signature migration:

```rust
pub struct Router<S = ()> {
    pending: HashMap<String, Vec<PendingRoute>>,
    inner: Option<matchit::Router<MethodMap>>,
    pub(crate) middlewares: Vec<Arc<dyn MiddlewareTrait>>,
    pub(crate) state_map: StateMap,
    pub(crate) fallback: Option<BoxedHandler>,
    pending_nests: Vec<(String, Router<S>)>,   // nests carry the same S until with_state
    // ... openapi fields ...
    _state: PhantomData<fn() -> S>,
}

impl<S> Router<S> where S: Clone + Send + Sync + 'static {
    pub fn new() -> Self { /* + _state: PhantomData */ }

    // route/get/post/... gain the S-preserving return type:
    pub fn get<H, T>(self, path: &str, handler: H) -> Self
    where H: Handler<T>, T: 'static { self.route(MethodKind::Get, path, handler) }
    // ... identical for post/put/delete/patch/head/options/on/any/fallback ...

    // nest: same-S sub-router (the common case)
    pub fn nest(mut self, prefix: &str, sub: Router<S>) -> Self { /* push */ self }

    // merge: requires identical S (coherence rule from rust.md)
    pub fn merge(mut self, other: Router<S>) -> Self { /* fold other.pending into self */ self }

    // THE state-binding transition: Router<S> -> Router<()>
    pub fn with_state(self, state: S) -> Router<()> {
        // materialize S into the dynamic state_map (so existing extraction still works),
        // then re-tag as Router<()> (state is now bound; no more S obligations).
        let mut r: Router<()> = self.retag();
        r.state_map.insert(TypeId::of::<S>(), Arc::new(state) as Arc<dyn AnyState>);
        r
    }
}
```
> `with_state` is the bridge between the typed world and the existing dynamic `StateMap`. After
> `with_state`, the router is `Router<()>` and `State<T>` extraction reads `T` out of the map exactly as
> today — **but** the *compile-time* check (B.4) has already guaranteed every `State<T>` in the tree
> satisfies `T: FromRef<S>`, so the runtime lookup can never miss. This is how we get "missing state = a
> compile error" without abandoning the working `TypeId` map.

`retag()` is a private helper that moves every field into a `Router<()>` (the data is `S`-independent;
only the `PhantomData` tag changes). `Default for Router<S>` mirrors `new`.

**`Neutron::router`** (`app.rs:399`) must take `Router<()>` (a fully-stated router). Update its signature;
`build_dispatch`/`into_service` already ignore `S` (they read the dynamic map). `nest_service` and the
opaque-service mount are unaffected (no `S`).

**CHECKPOINT B.3:** This is the big one. `cargo test -p neutron` — **expect a cascade of inference
failures** at call sites that wrote `Router::new()` with no state: they now infer `S = ()`, which is fine,
but any test that mixed `.state(x)` (the old dynamic API) must be reconciled. **Keep `.state<T>()`** as the
dynamic escape hatch (the plan's B.4 "escape hatch") so old tests compile unchanged — it inserts into
`state_map` without touching `S`. Verify the full existing router test module passes.

### B.4 — `State<T>: FromRequestParts` gated on `T: FromRef<S>` (the compile-time check)

The plan wants `State<T>` extraction to require `T: FromRef<S>`. But extractors don't carry `S` (the
`Handler` bound is `S`-free). The buildable resolution on this tree: keep `State<T>` runtime extraction as
today (reads the dynamic map, B.3 guarantees presence), and add the **compile-time** obligation through the
router's handler-registration bound, where `S` *is* in scope. Concretely, `route<H,T>` gains a
`where` clause asserting each `State<_>` in `T` is `FromRef<S>` — implemented via a marker trait
`HandlerState<S>` auto-derived for tuples whose `State<U>` members satisfy `U: FromRef<S>`. (This mirrors
Axum's `Handler<T, S>` second type param, scoped down to the state check.)

> **Honest scoping note:** full Axum-style `Handler<T, S>` threading is larger than this spec's budget. The
> minimum that delivers "missing state is a compile error" is: a `#[debug_handler]`-adjacent bound or a
> `route` `where State<U>: FromRef<S>`-style clause. If the marker-trait approach proves too invasive in
> one PR, the **documented fallback** is: `with_state` consumes `S` and the trybuild test asserts that
> requesting `State<Db>` without `Db: FromRef<AppState>` fails at `with_state` time. Either way the failure
> is at compile time, which is the acceptance criterion.

**CHECKPOINT B.4:** runtime test `typed_state_extracts` (build `AppState`, `.with_state(app)`, handler
takes `State<Db>`, returns db field) green.

### B.5 — trybuild UI tests (the compile-error guarantees)

`neutron/tests/ui.rs`:
```rust
#[test]
fn ui() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/missing_state_is_compile_error.rs");
    t.compile_fail("tests/ui/merge_requires_same_state.rs");
    t.pass("tests/ui/derive_from_ref_ok.rs");
}
```
- `missing_state_is_compile_error.rs`: `Router::<AppState>::new().get("/", |_: State<Unrelated>| async {})`
  then `.with_state(app)` — fails because `Unrelated: FromRef<AppState>` is unsatisfied. Snapshot the
  error mentions `FromRef`.
- `merge_requires_same_state.rs`: `Router::<A>::new().merge(Router::<B>::new())` — fails on `S` mismatch.
- `derive_from_ref_ok.rs`: a passing `#[derive(FromRef)]` + `with_state` + `State<substate>` handler.

**CHECKPOINT B.5:** `cargo test -p neutron` green including trybuild; `cargo test -p neutron-macros` green.

### B.6 — Migrate `app.rs`, `testing.rs`, examples, prelude

- `Neutron::router(Router<()>)` — examples that did `Router::new()....` and passed to `.router()` now must
  end with `.with_state(())` only if they used typed state; pure-`()` routers infer `S=()` and pass
  unchanged. Add `.with_state(app_state)` to any example using typed state.
- `prelude.rs`: export `FromRef`, `State` (already), `Router`.
- Confirm `RouterService`/`into_service` still take `self` (now `Router<()>` in production).

**FINAL CHECKPOINT (Part B):** `cargo build --workspace`, `cargo test -p neutron`, `cargo test -p
neutron-macros`, `--all-features`, and `cargo clippy --workspace --all-features -- -D warnings` all clean.

---

## Ordering summary (do not reorder)

```
A.0 decide trait shape (no code)
A.1 async trait + macro F:Clone + tests await        [GREEN]
A.2 Request.body_stream + collect_body (transitional)[GREEN]
A.3 body extractors -> collect_body (buffered)       [GREEN]
A.4 BodyStream extractor                              [GREEN]
A.5 7 collect sites -> ReqBody pass-through           [GREEN]
A.6 remove Bytes field; collect_body fully streaming  [GREEN]
A.7 h3 + protocol-parity verification                 [GREEN]
---- Part A shippable ----
B.0 neutron-macros crate (PREREQUISITE)               [GREEN]
B.1 FromRef trait                                     [GREEN]
B.2 #[derive(FromRef)]                                [GREEN]
B.3 Router<S=()> thread the generic                   [GREEN]
B.4 State<T> gated on FromRef<S>                       [GREEN]
B.5 trybuild UI tests                                 [GREEN]
B.6 migrate app/testing/examples/prelude              [GREEN]
```
