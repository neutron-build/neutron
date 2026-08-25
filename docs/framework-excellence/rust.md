# Neutron Rust Framework — Implementation Scaffold to 10/10

> Standalone, phase-by-phase engineering plan. Execute top to bottom. Every cited line
> number was verified against the tree on the audit branch; where the original audit drifted,
> the corrected location is given inline. Nothing here modifies framework source — it is the
> spec a developer implements against.

---

## Framing

**Current: 7.5/10. Target: 10/10.** Neutron Rust already ships real HTTP/1+2+3, WS/SSE/TLS
(server-side), matchit radix routing, Axum-parity-ish extractors, RFC 7807 errors, OpenAPI,
thread-per-core + SO_REUSEPORT, and granular Cargo features. The gap to Axum (the bar) is not
feature count — it is **architectural coherence** plus **three correctness/compatibility
defects that contradict the headline thesis**.

Five foundations are missing or broken:

1. **`Router` is not a `tower::Service`** (`router.rs:220` — `pub struct Router` has no params,
   no `Service` impl) and **extractors are synchronous over a pre-buffered body**
   (`extract.rs:68-69` — `fn from_request(req: &Request) -> Result<Self, Response>`). These two
   facts force a lossy/buffering Tower bridge, no streaming request bodies, no `oneshot` testing,
   and no inheritance of the tower-http ecosystem.
2. **Two parallel middleware abstractions.** `MiddlewareTrait` (`middleware.rs:25` —
   `async fn(Request, Next) -> Response`) is *not* `tower::Service`. After we add a Tower-native
   router we will have **two non-composing middleware systems** unless we deliberately reconcile
   them. Axum has exactly one. This is the coherence problem the scaffold must actually solve,
   not just gesture at.
3. **Built-in failures bypass `AppError`** (extractor rejections emit plain-text
   `(400, "Invalid JSON")` etc.), so the framework violates its own RFC 7807 contract.
4. **The Nucleus client is plaintext-only** (`neutron-nucleusdb/src/pool.rs:182` —
   `tokio_postgres::connect(&cs, NoTls)`). This makes the headline claim "any Postgres client
   works / strictest correctness" **provably false**: it cannot connect to any TLS-required
   Postgres (Neon, Supabase, RDS-with-SSL, most managed PG) and ships credentials + all data in
   cleartext. This is simultaneously a **security defect and a compatibility defect** and is
   promoted to **P0**.
5. **`State<T>` missing-state is a runtime 500, not a compile error** — there is no `FromRef`
   trait and no `Router<S>` generic. This is the single hardest, most load-bearing type-system
   change in the plan and gets a dedicated phase.

**Thesis for beating the leader.** Match Axum on its own terms (Tower-native router, async
extractors, typed rejections, compile-time state via `Router<S>`/`FromRef`, `#[debug_handler]`,
tuple `IntoResponse`), then exceed it on three axes Axum cannot easily claim:

- **HTTP/3 in the core** — and every architectural change below is acceptance-gated on
  HTTP/1 **and** /2 **and** /3, so the parity is real and doesn't rot.
- **The strictest correctness story** — every built-in error is RFC 7807; a 14-model database
  client with protocol-correct collection decoding (no `,`/`=` corruption) over a **TLS-capable**
  connection; a contract-conformance suite shared across SDKs.
- **A canonical, order-enforced middleware stack** that no Rust framework ships out of the box —
  expressed as real `tower::Layer`s so it inherits `tower::limit`, `load_shed`, `buffer`, and all
  of tower-http.

Axum gives you Lego bricks; Neutron gives you the bricks *and* the correct, tested assembly.

### Phase ordering (corrected — the build dependency chain is load-bearing)

```
P0  (correctness + TLS; no architecture change; independently shippable)
        │
P1.1  Router : tower::Service          (keystone)
        │
P1.M  Reconcile MiddlewareTrait → tower::Layer   (coherence; was unspecified)
        │
P1.5  Async extractors + typed RFC 7807 rejections   ← MUST precede P1.2
        │
P1.6  Router<S> + FromRef + #[derive(FromRef)] + State   (type-system surgery)
        │
P1.2  Streaming request bodies        (needs async extractors to .await collect)
        │
P1.3  Lossless non-buffering Tower bridge   (falls out of P1.1+P1.2+P1.M)
        │
P1.4  default_stack() as tower::Layers + order test
        │
P1.7  #[debug_handler]   (coupled to P1.5+P1.6; sequenced last in P1)
        │
P2   Differentiation (errors-through-AppError, OpenAPI-derive, validation,
     conformance suite, real-engine integration tier, benchmarks, MethodRouter)
```

> The original draft sequenced P1.2 (streaming) **before** P1.5 (async extractors) — unbuildable,
> because body extractors cannot `.await` a collect until the trait is async. Fixed above.

---

## P0 — Correctness, contract, and security (no architecture change)

Independently shippable, low-risk, and unblock the conformance suite.

### P0.0 — TLS-capable Nucleus connections (promoted; the thesis depends on it)

- **File:** `rust/crates/neutron-nucleusdb/src/pool.rs:8` (`use ... NoTls`), `:182`
  (`tokio_postgres::connect(&cs, NoTls)`), plus the connection-string parser.
- **Bug:** Plaintext-only. Cannot reach any `sslmode=require` server; transmits credentials and
  all rows in cleartext. Directly contradicts "any Postgres client works."
- **Change:**
  1. Add `tokio-postgres-rustls` + `rustls` (+ `rustls-native-certs` or `webpki-roots`) behind a
     **default-on** `tls` feature.
  2. Parse `sslmode` from the connection string / `DATABASE_URL`
     (`disable | prefer | require | verify-ca | verify-full`). Default to `prefer`
     (TLS if offered, plaintext otherwise) to preserve local-dev ergonomics; `require`+ enforce.
  3. Build a `MakeTlsConnect` from rustls with the OS/native root store; honor `verify-full`
     (SNI + hostname verification) vs `verify-ca`. Keep `NoTls` only for `sslmode=disable`.
  4. Surface a typed `NucleusError::Tls(..)` for handshake/verification failures.
- **Why:** Security + compatibility. Without it, P2.5's "any Postgres client works" pillar is a
  false claim and the framework cannot be deployed against managed Postgres.
- **Tests (integration tier, P2.5):**
  - `connects_to_tls_required_server` — against a Postgres container configured `ssl=on` with
    `sslmode=require`; asserts a successful round-trip.
  - `sslmode_disable_uses_plaintext`, `verify_full_rejects_bad_hostname`,
    `sslmode_parsed_from_url`.
- **Acceptance:** A `sslmode=require` server is reachable; `sslmode` is parsed and enforced;
  `NoTls` is reachable only under `disable`; default build links rustls.

### P0.1 — Remove dead `unsafe impl Send/Sync` on `HandlerWrapper`

- **File:** `rust/crates/neutron/src/handler.rs:531-532`.
- **Change:** Delete:
  ```rust
  unsafe impl<H: Send, T> Send for HandlerWrapper<H, T> {}
  unsafe impl<H: Sync, T> Sync for HandlerWrapper<H, T> {}
  ```
  `HandlerWrapper { handler: H, _marker: PhantomData<fn() -> T> }` — `PhantomData<fn() -> T>` is
  `Send + Sync` for all `T`, so the wrapper auto-derives `Send`/`Sync` whenever `H: Send`/`Sync`.
  The `unsafe` asserts exactly what auto-derive already gives; removing it cannot regress.
- **Why:** Audit MEDIUM (e). Unnecessary `unsafe` in a framework claiming best-in-class is a
  `cargo-geiger` red flag and a credibility cost.
- **Tests — must actually instantiate (the draft's test was green-by-vacuity):**
  ```rust
  // Top-level, evaluated at compile time — not an uncalled inner fn.
  const _: fn() = || {
      fn assert_send_sync<T: Send + Sync>() {}
      // Instantiate with a concrete handler + concrete arg tuple:
      assert_send_sync::<HandlerWrapper<fn() -> &'static str, ()>>();
  };
  ```
- **Acceptance:** `unsafe impl`s deleted; `cargo build`/`test` pass; `cargo clippy -- -D warnings`
  clean; the assertion is actually instantiated (fails to compile if the bound regresses).

### P0.2 — `Router::resolve()` must never panic before `build()`

- **File:** `rust/crates/neutron/src/router.rs:602` (`resolve`), and the `.expect("Router not
  built …")` it delegates through.
- **Current:** `self.inner.as_ref().expect(...)` — a request-path panic if `resolve` runs before
  `build()`/`ensure_built()`.
- **Change — design (B), the simple one (the draft's preferred `OnceLock`+`Mutex` design (A) is
  over-engineered for a non-production panic the server already prevents):**
  - Add `RouteError::NotBuilt`. `resolve` returns `Err(RouteError::NotBuilt)` instead of
    panicking when `inner` is `None`. The server already force-builds in `listen()`/`ensure_built`,
    so production never sees it; benchmarks/direct callers get a `Result`, not an unwind.
  - Optionally `debug_assert!(self.inner.is_some(), "call build() before resolve()")` to keep the
    contract loud in debug without panicking in release.
- **Why:** Audit LOW (router.rs:603). A request-path panic is unacceptable; Axum's router never
  panics on dispatch.
- **Tests:** `resolve_before_build_returns_not_built` — construct a `Router`, call `resolve`
  without building, assert `Err(RouteError::NotBuilt)`, never unwinds (wrap in
  `std::panic::catch_unwind` to prove no panic).
- **Acceptance:** No reachable `.expect()`/`panic!()`/`unwrap()` from `resolve()`; test green.

### P0.3 — 405 responses must carry the `Allow` header

- **Files:** `rust/crates/neutron/src/router.rs:106` (`RouteError`), the `resolve_matched`
  not-allowed path (`:648+`), `MethodMap` (`:61+`), and the dispatch site in `app.rs` that maps
  `RouteError` → response.
- **Change:**
  1. `MethodMap::allowed() -> SmallVec<[Method; 7]>` returning methods with `Some` handlers
     (+ `HEAD` if `GET` present, + `OPTIONS`).
  2. `RouteError::MethodNotAllowed { allow: SmallVec<[Method; 7]> }` (was a unit variant).
  3. Populate `allow` in `resolve_matched` on the not-allowed path.
  4. At dispatch, emit `405` with `Allow: GET, POST, …` routed through
     `AppError::method_not_allowed(allow)` (see P2.1).
- **Why:** RFC 7231 requires `Allow` on 405; Axum and Actix both emit it.
- **Tests:** `method_not_allowed_sets_allow_header` — GET+POST on `/users`, `DELETE /users` →
  405 with `Allow` ⊇ {GET, POST, HEAD, OPTIONS} (order-insensitive). Update existing
  `matches!(…, Err(RouteError::MethodNotAllowed))` tests to the struct form.
- **Acceptance:** 405 always sets a correct `Allow`; conformance test (P2.4) asserts it on
  HTTP/1, /2, /3.

### P0.4 — `GET /health` returns the exact contract shape

- **File:** `rust/crates/neutron/src/health.rs` (extend; keep `/healthz`/`/readyz`).
- **Contract:** `GET /health` → `{ "status", "nucleus", "version" }`.
- **Change:** Add a `contract()` handler builder:
  ```rust
  impl HealthCheck {
      /// Contract GET /health handler: { status, nucleus, version }.
      /// `nucleus`: "connected" | "disconnected" | "unconfigured" (optional probe).
      /// `version`: env!("CARGO_PKG_VERSION") of the host crate, injected by the caller.
      pub fn contract(&self, nucleus_probe: Option<CheckFn>, version: &'static str)
          -> impl Fn() -> Pin<Box<dyn Future<Output = Response> + Send>> + Clone + Send + Sync + 'static;
  }
  ```
  Body: `{"status":"ok"|"degraded","nucleus":"connected"|"disconnected"|"unconfigured","version":"0.1.0"}`.
  `status` is `"ok"` when the probe passes or is unconfigured, `"degraded"` on failure. Keep
  `/healthz` (liveness, always 200) and `/readyz` (full checks) as Kubernetes aliases.
- **Why:** The contract is the cross-SDK invariant; every SDK's `/health` must be shape-identical.
- **Tests:** `health_contract_shape` (exactly three keys, correct types),
  `health_degraded_when_nucleus_down`; keep all healthz/readyz tests.
- **Acceptance:** `/health` returns the three-key object; conformance test (P2.4) validates it.

### P0.5 — KV collection reads must not split on `,` / `=` (client-side fix only)

- **File:** `rust/crates/neutron-nucleusdb/src/models/kv.rs:215, 299-300, 354, 409, 429`
  (`lrange`/`hgetall`/`smembers`/`zrange`/`zrangebyscore`).
- **Bug (confirmed):** `raw.split(',')` and `split_once('=')` corrupt any value containing `,` or
  `=`. Ported cross-SDK bug.
- **Scope correction:** The "preferred server-side fix" (changing Nucleus's `KV_*` SQL functions
  to return `text[]`/`jsonb`) is a **database-engine change in another crate's protocol surface**,
  not a Rust-framework fix. It is tracked separately as **N-1** (below) with its own acceptance.
  The in-scope P0 work is the **client-side decode**.
- **Client-side fix (in scope, P0):** Have Nucleus emit a `jsonb` payload for these reads and
  parse with `serde_json` — never `split`. Centralize in exactly two private helpers so the bug
  cannot be re-introduced per-call-site:
  ```rust
  fn decode_collection(row: &Row, col: usize) -> Result<Vec<String>, NucleusError> {
      let v: serde_json::Value = row.try_get(col)?;          // jsonb array
      Ok(serde_json::from_value(v)?)
  }
  fn decode_hash(row: &Row, col: usize) -> Result<HashMap<String, String>, NucleusError> {
      let v: serde_json::Value = row.try_get(col)?;          // jsonb object
      Ok(serde_json::from_value(v)?)
  }
  ```
  The five call sites delegate; no `split(',')` remains in `kv.rs`.
- **Tests (real engine — P2.5):**
  - `lrange_preserves_commas` — push `"a,b"`, `"c=d"`, `"emoji 🚀"`, `"line\nbreak"`; exact
    round-trip.
  - `hgetall_preserves_equals_in_value` — `HSET k f "x=y,z"` → value is `"x=y,z"`.
  - `smembers_unicode`, `zrange_with_commas`.
- **Acceptance:** All collection reads round-trip arbitrary bytes (`,`, `=`, newline, unicode);
  `grep "split(','" kv.rs` returns nothing.

#### N-1 (separately-tracked Nucleus engine item) — server-side typed collection returns

- **Surface:** Nucleus `KV_LRANGE` / `KV_SMEMBERS` / `KV_ZRANGE` / `KV_ZRANGEBYSCORE` /
  `KV_HGETALL` SQL functions.
- **Change:** Return a typed `text[]` / `jsonb` array (or `jsonb` object for `HGETALL`) instead of
  a comma-joined string. Provide a one-release compat window: emit `jsonb` (which the P0.5 client
  already reads) before any `text[]` migration so client and server never skew.
- **Acceptance (engine repo):** functions return typed arrays; a Nucleus-side test asserts a
  value containing `,`/`=`/newline survives the function unchanged. **Out of scope for this
  Rust-framework PR**; the client (P0.5) is correct regardless of which representation the engine
  emits, as long as it emits `jsonb`.

### P0.6 — Fix the wrong middleware order in the example

- **File:** `rust/crates/neutron/examples/rest_api.rs` (the hand-wired
  `Logger → RequestId → Cors` stack).
- **Bug:** Contract requires **RequestID before Logging**.
- **Change:** Replace the hand-wired stack with `default_stack()` (P1.4) once it lands; in the
  interim reorder to `RequestId → Logger → Cors` with a comment pointing at
  `FRAMEWORK_CONTRACT.md`.
- **Why:** Examples are copy-pasted; a wrong example propagates the bug into every user app.
- **Acceptance:** Example compiles, runs, observed order matches the contract; ideally just calls
  `default_stack()`.

---

## P1 — Fundamentals to match the leader (Axum)

### P1.1 — Make `Router` a `tower::Service` (the keystone)

- **Files:** `rust/crates/neutron/src/router.rs` (add `RouterService` via
  `Router::into_service()`); `rust/crates/neutron/src/app.rs` (dispatch through it);
  `rust/crates/neutron/src/tower_compat.rs` (collapses — see P1.3).
- **Design:** Keep `Router` as the ergonomic *builder*; add a compiled, cloneable service:
  ```rust
  #[derive(Clone)]
  pub struct RouterService {
      inner: Arc<RouterInner>, // matchit router + middleware chain + state + fallback
  }
  impl tower_service::Service<http::Request<Body>> for RouterService {
      type Response = http::Response<Body>;
      type Error = Infallible;                 // infallible at the boundary, like Axum
      type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Infallible>> + Send>>;
      fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
          Poll::Ready(Ok(()))                  // leaf is always ready; see P1.M for backpressure
      }
      fn call(&mut self, req: http::Request<Body>) -> Self::Future { /* resolve → chain → response */ }
  }
  impl Router { pub fn into_service(self) -> RouterService { /* ensure_built + freeze */ } }
  ```
  - Boundary request type is `http::Request<Body>` (streaming `Body`, P1.2). `NeutronRequest` is
    built inside `call` from parts + `Body`, carrying `Extensions` and the `StateMap` via
    `http::Extensions`.
  - Add `nest_service(prefix, svc)` to mount any `tower::Service`, superseding build-time
    `flatten_nests` re-wrapping for the opaque-service case. Keep `flatten_nests` for the
    same-`Router` merge fast path.
  - **Backpressure:** the leaf returns `Ready`, but P1.M routes all middleware through the Tower
    stack so `tower::limit`/`load_shed`/`buffer` readiness propagates above the leaf — making the
    "inherit tower-http ecosystem" claim real rather than hollow.
- **Tests:**
  - `router_is_tower_service` — `fn _a<S: tower::Service<http::Request<Body>>>(){}` on
    `RouterService`.
  - `router_oneshot_dispatch` — `router.into_service().oneshot(req)` returns the expected response
    through the *real* middleware chain.
  - `nest_service_mounts_opaque_service` — mount a hand-written `tower::Service` under `/proxy`;
    assert it receives requests with extensions intact.
- **Acceptance:** `RouterService: tower::Service<http::Request<Body>, Response =
  http::Response<Body>, Error = Infallible>`; all existing router tests pass; `oneshot` works;
  **dispatch verified on HTTP/1, /2, and /3** (the h3 loop in `http3_server.rs` routes through the
  same `RouterService`).

### P1.M — Reconcile `MiddlewareTrait` with `tower::Layer` (coherence; previously unspecified)

- **Files:** `rust/crates/neutron/src/middleware.rs` (`MiddlewareTrait`, `Next`),
  `rust/crates/neutron/src/middleware/` stack module.
- **Problem:** After P1.1 there are two middleware models — `MiddlewareTrait`
  (`async fn(Request, Next) -> Response`) and `tower::Layer`/`Service`. Left unreconciled they
  don't compose; `default_stack` (P1.4) and the Tower bridge (P1.3) would fight.
- **Decision — adopt Axum's model: ONE abstraction (`tower::Layer`/`Service`),
  `MiddlewareTrait` becomes sugar that lowers to it.**
  1. Provide `from_fn(f)` / `from_fn_with_state(s, f)` where `f: async fn(Request, Next) ->
     Response`, returning a `tower::Layer` (mirrors `axum::middleware::from_fn`). `Next` becomes a
     thin wrapper over the inner `tower::Service`'s `call`.
  2. Keep the existing `MiddlewareTrait` public API as a **compatibility facade** that internally
     constructs a `from_fn` layer, so existing user code keeps compiling, but everything executes
     as one Tower stack.
  3. `default_stack` (P1.4) is then specified purely in terms of `tower::Layer`s; user
     `async fn` middleware slots in via `from_fn`.
- **Why:** This is *the* coherence fix the scaffold exists to deliver. One middleware model means
  user middleware, contract layers, and the whole of tower-http compose without a second adapter.
- **Tests:**
  - `from_fn_middleware_runs_in_tower_stack` — an `async fn` middleware added via `from_fn`
    observes the request and mutates the response inside a `RouterService` stack.
  - `legacy_middleware_trait_still_works` — an existing `MiddlewareTrait` impl runs unchanged.
  - `from_fn_and_tower_layer_compose` — a `from_fn` layer and a real `tower::Layer`
    (e.g. `tower_http::trace::TraceLayer`) compose in one stack, order preserved.
- **Acceptance:** There is exactly one execution model (Tower); `MiddlewareTrait`/`Next` are sugar
  over it; user `async fn` middleware and tower-http layers compose; no second middleware runtime.

### P1.5 — Async extractors with typed, RFC 7807 rejections (precedes streaming)

- **Files:** `rust/crates/neutron/src/extract.rs` (whole file; trait at `:59`/`:68-69` is sync
  today), `rust/crates/neutron/src/error.rs` (add `From` impls + rejection→`AppError`).
- **Design:**
  1. **Make the traits async** (native AFIT/RPITIT, stable since Rust 1.75; works on
     **edition 2021**, which this workspace uses — `Cargo.toml:26` `edition = "2021"`. No
     `async_trait` needed, matching Axum 0.8's move):
     ```rust
     pub trait FromRequestParts: Sized + Send {
         type Rejection: IntoResponse;
         async fn from_parts(parts: &mut RequestParts) -> Result<Self, Self::Rejection>;
     }
     pub trait FromRequest: Sized + Send {
         type Rejection: IntoResponse;
         async fn from_request(req: Request) -> Result<Self, Self::Rejection>;
     }
     ```
     Keep the blanket `FromRequestParts → FromRequest`.
  2. **Typed rejections through `AppError`:** `JsonRejection`, `PathRejection`, `QueryRejection`,
     `FormRejection`, `StateRejection`, `BytesRejection`. Each `impl IntoResponse` builds an
     `AppError` → `application/problem+json`. This kills the plain-text `(400, "Invalid JSON: …")`
     and the `(500, "Internal Server Error")` rejections that exist today.
  3. **Replace hand-rolled `Path` tuple impls** with a serde `Deserializer` over the named-param
     map (Axum's approach) → arbitrary arity + named-struct paths for free. Keep the scalar fast
     path.
  4. **Keep `Optional<T>` and `TypedHeader<T>`** — genuine strengths (base Axum moved TypedHeader
     to `axum-extra`).
- **Tests:**
  - `json_rejection_is_problem_json` (415/400, `content-type: application/problem+json`, RFC 7807
    body), `path_rejection_is_problem_json`, `query_rejection_is_problem_json`,
    `missing_state_rejection_is_problem_json` (until P1.6 makes it a compile error).
  - `path_arbitrary_arity` — `Path<(A,B,C,D,E)>` and `Path<NamedStruct>` deserialize.
  - Preserve every existing extractor test (statuses unchanged; only content-type/body upgrade).
- **Acceptance:** Every extractor rejection emits `application/problem+json`; `Path` supports
  arbitrary arity + structs; traits are async; `Optional`/`TypedHeader` retained.

### P1.6 — Compile-time-checked state: `Router<S>` + `FromRef` + `#[derive(FromRef)]`

> The single largest mechanical change in the plan. It re-types `route`, `nest`, `merge`,
> `with_state`, and the handler bound. Budgeted as its own phase with the full trait surface,
> because the draft's one-bullet version would not compile.

- **Files:** `rust/crates/neutron/src/router.rs:220` (introduce `Router<S = ()>`),
  `rust/crates/neutron/src/extract.rs` (`State<T>` reads typed state),
  new `rust/crates/neutron-macros/` (`#[derive(FromRef)]` + later `#[debug_handler]`),
  `rust/crates/neutron/src/from_ref.rs` (new — the trait).
- **Design:**
  1. **Introduce the trait (does not exist today — confirmed no `FromRef` in tree):**
     ```rust
     pub trait FromRef<S> { fn from_ref(state: &S) -> Self; }
     impl<S: Clone> FromRef<S> for S { fn from_ref(s: &S) -> S { s.clone() } } // identity
     ```
  2. **`#[derive(FromRef)]`** generates substate extraction (the entire point of `FromRef`):
     ```rust
     #[derive(Clone, FromRef)]
     struct AppState { db: Db, cache: Cache } // yields FromRef<AppState> for Db and Cache
     ```
  3. **`Router<S = ()>`** generic over a state type, threaded via `with_state(s: S) -> Router<()>`.
     `State<T>: FromRequestParts<S>` where `T: FromRef<S>` — **missing state is a compile error**,
     not a runtime 500.
     - `route`, `nest`, `merge`, fallback all become `Router<S>`-preserving.
     - **Merge/nest state-coherence rule:** merging two `Router<S>` requires identical `S`;
       nesting a `Router<S2>` into `Router<S>` requires `S2: FromRef<S>` (or `with_state` already
       applied to make it `Router<()>`). Document this rule and enforce it via the type signatures.
  4. **Escape hatch:** keep the dynamic `TypeId` state map as opt-in (`Extension`-style) for
     advanced/multi-state cases, but the default, type-checked path is `with_state`.
- **Tests:**
  - `typed_state_extracts` (runtime), `derive_from_ref_extracts_substate`.
  - `trybuild` UI test `missing_state_is_compile_error.rs` — asserts the build fails with a clear
    message when `State<Db>` is requested but `Db: FromRef<S>` is unsatisfied.
  - `merge_requires_same_state.rs` (trybuild) — mismatched `S` fails to compile.
- **Acceptance:** `Router<S>` compiles and threads state through `route`/`nest`/`merge`/
  `with_state`; missing state is a build error; `#[derive(FromRef)]` extracts substates; dynamic
  escape hatch still available.

### P1.2 — Streaming request bodies by default (after async extractors)

- **Files:** `rust/crates/neutron/src/handler.rs` (`NeutronRequest` stores `Body`, not `Bytes`),
  `rust/crates/neutron/src/app.rs` **at all three collect sites** (`:313`, `:677`, `:1006` —
  confirmed; the draft named only `677`), `rust/crates/neutron/src/http3_server.rs`,
  `rust/crates/neutron/src/extract.rs`.
- **Design:**
  1. `NeutronRequest` holds `body: Body` (`http_body::Body`) instead of `Bytes`. Add
     `fn into_body(self) -> Body` and `async fn collect_body(&mut self, limit: usize) ->
     Result<Bytes, BodyError>`.
  2. **Stop pre-collecting.** Delete the `Limited::new(body, body_limit).collect().await` at **all
     three** dispatch sites (`app.rs:313` HTTP/1, `:677` HTTP/2, `:1006` TLS/h3 path) and pass the
     streaming body straight onto `NeutronRequest`. Keep the cheap `Content-Length > limit`
     early-reject and the bodyless fast path.
  3. **Body-buffering extractors collect themselves** (now possible — P1.5 made the trait async):
     `Bytes`, `String`, `Json<T>`, `Form<T>` call `req.collect_body(limit).await`. Enforce a
     **streaming byte-limit** during collection (count frames as they arrive), replacing the
     pre-collect cap so it composes with streaming and returns **413 mid-stream**.
  4. Add a `BodyStream` / `Body`-typed `FromRequest` exposing `into_data_stream()` for
     frame-level, backpressured consumption; "body extractor is last" is enforced by the single
     `FromRequest` rule.
  5. Reach the **HTTP/3 path** (`http3_server.rs`) with the same streaming `Body` so uploads work
     over h3.
- **Tests:**
  - `large_upload_streams_without_buffering` (peak memory bounded, frame-by-frame),
    `body_limit_enforced_during_stream` (413 mid-stream),
    `json_extractor_still_works_over_stream`,
    `h3_streaming_upload` (feature-gated).
  - **`no_body_collect_in_any_dispatch_path`** — a grep/structural test asserting
    `Limited::new(..).collect()` appears in **zero** of the three sites.
  - **Perf gate (ties to P2.6):** `small_json_extract_latency` micro-bench — sub-1KB JSON extract
    latency **pre vs post** P1.2; assert the streaming/frame-counting path does **not** regress the
    common small-body case (the 99%) while chasing large uploads (the 1%).
- **Acceptance:** No unconditional request-body `.collect()` in any of the three dispatch paths
  (grep-enforced); `Bytes`/`String`/`Json`/`Form` collect lazily with a streaming limit;
  `BodyStream` exists; small-body extract latency not regressed; **verified on HTTP/1, /2, /3.**

### P1.3 — Lossless, non-buffering Tower bridge (or delete it)

- **File:** `rust/crates/neutron/src/tower_compat.rs` (`neutron_to_http_request` drops `StateMap`
  + extensions; `http_request_to_neutron` `collect()`s the body on every layer).
- **Change:** Once P1.1 + P1.M + P1.2 land, the native path *is* a Tower stack and the buffering
  bridge largely disappears. Concretely:
  1. Carry state + extensions across the boundary via `http::Extensions` on the outgoing
     `http::Request`; read them back in `http_request_to_neutron`.
  2. **Pass `Body` through** — no `collect()`. The conversion is parts-only.
  3. Apply contract layers as real `tower::Layer`s wrapping tower-http where it exists
     (`CompressionLayer`, `TimeoutLayer`, `CorsLayer`, `SetRequestIdLayer`, `TraceLayer`) so
     Neutron *inherits* the ecosystem instead of reimplementing it.
- **Tests:** `tower_layer_sees_extensions`, `tower_layer_does_not_buffer_body` (counting/poison
  body that proves frames flow lazily), `tower_http_compression_roundtrip`.
- **Acceptance:** Tower layers observe full request state + extensions; no body buffering in the
  bridge; at least one real tower-http layer works end-to-end; verified on HTTP/1, /2, /3.

### P1.4 — Canonical `default_stack()` as `tower::Layer`s + order-enforcing test

- **Files:** new `rust/crates/neutron/src/middleware/stack.rs`; export from `prelude`; wire into
  `examples/rest_api.rs` (P0.6).
- **Design (Tower-native — uses P1.M):** A single builder that applies the contract order as real
  layers:
  ```rust
  /// Contract order (FRAMEWORK_CONTRACT.md):
  /// RequestID → Logging → Recovery → CORS → Compression → RateLimit → Auth → Timeout → OpenTelemetry.
  pub struct DefaultStack { /* per-layer toggles + config */ }
  impl DefaultStack {
      pub fn new() -> Self;
      pub fn cors(self, c: CorsConfig) -> Self;
      pub fn auth(self, a: impl AuthLayer) -> Self;
      pub fn rate_limit(self, r: RateLimitConfig) -> Self;
      pub fn apply<S>(self, router: Router<S>) -> Router<S>; // fixed order, returns same state type
  }
  pub fn default_stack() -> DefaultStack { DefaultStack::new() }
  ```
  Order is **hard-coded inside `apply`** (a `tower::ServiceBuilder` chain) — the user configures
  layers but cannot reorder them. RequestID strictly precedes Logging so every log line carries the
  request id. Backpressure-capable layers (rate-limit, timeout) propagate readiness through the
  stack (P1.M).
- **Tests — the order assertion is the highest-leverage test in the scaffold:**
  ```rust
  #[tokio::test]
  async fn default_stack_runs_layers_in_contract_order() {
      // Each layer pushes its name to a shared Vec<&str> on entry and on exit.
      // Assert entry order == [RequestID, Logging, Recovery, CORS, Compression,
      //   RateLimit, Auth, Timeout, OpenTelemetry] and exit order == reverse.
  }
  ```
  Plus `request_id_precedes_logging`, `default_stack_health_and_cors_present`.
- **Acceptance:** `default_stack().apply(router)` produces the contract order; the ordering test
  records and asserts both entry and reversed-exit sequences; `examples/rest_api.rs` uses it.

### P1.7 — `#[debug_handler]` (coupled to P1.5 + P1.6; sequenced last in P1)

- **Files:** `rust/crates/neutron-macros/` (proc-macro), with the `Handler`/`FromRequest` bound
  structure from P1.5/P1.6 engineered so failures point at the right argument.
- **Design:** `#[debug_handler]` (Axum 0.8 parity): when a handler fails the `Handler` bound,
  expand to code that surfaces *which extractor* breaks and why (e.g. "`Json<T>` must be the last
  argument", "`T: FromRef<S>` not satisfied for `State<Db>`"). This is **not** a standalone macro
  — good diagnostics require the `Handler` trait's error spans and bound structure (designed in
  P1.5/P1.6), so this item lands after them.
- **Tests:** `trybuild` snapshots — `debug_handler_explains_bad_extractor_order.rs`,
  `debug_handler_explains_missing_from_ref.rs`.
- **Acceptance:** `#[debug_handler]` emits a targeted diagnostic naming the offending argument;
  trybuild snapshots green.

### P1.8 — Response-side ergonomics: tuple `IntoResponse` (Axum's terse-handler core)

- **Files:** `rust/crates/neutron/src/response.rs` (or wherever `IntoResponse` lives).
- **Design:** Verify/implement the response-composition impls users touch most:
  `(StatusCode, T)`, `(StatusCode, HeaderMap, T)`, `(StatusCode, [(HeaderName, HeaderValue); N], T)`,
  and `Result<T, E>` where both arms `IntoResponse`. Without these, handlers can't return
  `(StatusCode::CREATED, Json(x))` and the API is below the bar on the half users touch most.
- **Tests:** `tuple_status_into_response`, `tuple_status_headers_into_response`,
  `result_into_response_both_arms`.
- **Acceptance:** Status-override and header tuples compose as return types; `Result<T, E>`
  composes.

### P1.9 — WebSocket / SSE survive the Service + streaming-body rework (regression guard)

- **Files:** `rust/crates/neutron/src/ws.rs`, `rust/crates/neutron/src/sse.rs`.
- **Problem:** WS upgrade needs the raw `hyper`/`Upgraded` handle; once `Router` is a
  `tower::Service` over streaming `Body`, the existing upgrade path may break. SSE must keep
  streaming responses through the new boundary.
- **Change:** Model `WebSocketUpgrade` as a `FromRequestParts` and `Sse` as `IntoResponse` (Axum
  parity), and verify both flow through `RouterService` + the streaming-body boundary.
- **Tests:** `ws_upgrade_roundtrip_through_router_service`, `sse_streams_through_router_service`,
  both on HTTP/1 and HTTP/2 (h3 WS where supported; otherwise documented N/A).
- **Acceptance:** WS upgrade and SSE round-trip through the new service stack with no regression.

---

## P2 — Differentiation to beat the leader

### P2.1 — Route every built-in failure through `AppError`

- **Files:** `rust/crates/neutron/src/app.rs` (404/405/413/timeout/panic-recovery responses),
  `error.rs`.
- **Design:** Replace `resp_payload_too_large()`, `resp_bad_request()`, and the 404/405 dispatch
  responses with `AppError` constructors: `payload_too_large`, `not_found`,
  `method_not_allowed(allow)` (P0.3), `timeout`, `internal` (panic recovery). Auto-populate
  `instance` with the request path (field already exists). Add `From` impls:
  `serde_json::Error → 400`, validation → `422` with `errors[]`, `NucleusError → 500/503`,
  `NucleusError::Tls → 503` (from P0.0).
- **Tests:** `not_found_is_problem_json`, `payload_too_large_is_problem_json`,
  `panic_recovery_is_problem_json_500`, `instance_is_request_path`.
- **Acceptance:** No built-in response is plain text; every error is RFC 7807 with `instance` set.

### P2.2 — Schema-deriving OpenAPI (single source of truth)

- **Files:** `rust/crates/neutron/src/openapi.rs`, `neutron-macros/` (`#[derive(ApiSchema)]`),
  `router.rs`.
- **Design:** `#[derive(ApiSchema)]` (or integrate `utoipa::ToSchema`) generating JSON Schema from
  `serde` types; route registration pulls method + path + request body + response type from the
  handler signature (utoipa-axum model). Keep the manual `ApiRoute` builder as an escape hatch.
- **Caveat (corrected):** The draft asserted a matchit/original **path divergence** at
  `router.rs:268` without a reproducing test. matchit and OpenAPI both use `{param}` syntax —
  **verify the divergence with a failing test first**; if both already use `{}`, drop the
  "store one path representation" item as busywork and keep only the schema-derivation work.
- **Tests:** `derived_schema_matches_serde_type`, `manual_apiroute_still_works`, and **only if a
  divergence is proven** `openapi_path_matches_registered_route`.
- **Acceptance:** Request/response schemas derive from `serde` types; manual escape hatch
  preserved; path-representation work included only if a real divergence is demonstrated.

### P2.3 — `ValidatedJson<T>` extractor (422 with field errors)

- **File:** `rust/crates/neutron/src/extract.rs`, optional `validation` feature.
- **Design:** `ValidatedJson<T>(pub T)` where `T: DeserializeOwned + Validate`
  (`validator`/`garde`). Deserialize, then `validate()`; on failure map each field error into
  `AppError`'s existing `errors: Vec<ValidationFieldError>` and return 422
  `application/problem+json`.
- **Tests:** `validated_json_rejects_with_422_and_field_errors`, `validated_json_accepts_valid`.
- **Acceptance:** Validation failures emit 422 with populated `errors[]`.

### P2.4 — Cross-SDK contract conformance suite (Rust slice)

- **Files:** new `rust/crates/neutron/tests/contract_conformance.rs`.
- **Design:** Table-driven suite asserting `FRAMEWORK_CONTRACT.md` invariants against a live
  `Neutron` app via `TestServer`: `GET /health` shape (P0.4), RFC 7807 content-type on every error
  class, 405 `Allow` header (P0.3), middleware order via `default_stack` markers (P1.4), graceful
  shutdown. **Run each HTTP-level assertion on HTTP/1, /2, and /3** so the protocol parity is
  conformance-tested, not assumed.
- **Graceful-shutdown harness (corrected — the draft had no mechanism):** spawn the server, open
  a request to a deliberately slow handler (`tokio::time::sleep` controllable via a barrier/oneshot
  the test holds), send SIGTERM (or call the internal shutdown trigger directly), assert (a) the
  in-flight request completes within the drain window, (b) new connections are refused during
  drain, (c) the default `shutdown_timeout` is 30s and is configurable. If a SIGTERM harness proves
  flaky in CI, downgrade the clause to "drain completes in-flight requests; timeout defaults to 30s
  and is configurable" — but the in-flight-completion assertion via the controllable handler is the
  real test and should be kept.
- **Acceptance:** Every contract clause has a passing assertion across HTTP/1, /2, /3; the
  shutdown test exercises a real in-flight drain; suite is referenceable by other SDKs.

### P2.5 — Real Nucleus/Postgres integration tier (testcontainers)

- **Files:** new `rust/crates/neutron-nucleusdb/tests/integration_kv.rs` (+ models);
  `dev-dependencies`: `testcontainers`.
- **Design:** Spin up Nucleus *and* stock Postgres in Docker; run every model's round-trip —
  especially the P0.5 corruption cases (`,`, `=`, newline, unicode) — and the **P0.0 TLS cases**
  (a `ssl=on` Postgres container, `sslmode=require`, `verify-full` hostname mismatch). Add a
  **feature-detection** path: detect Nucleus (contract feature-detection query); on plain Postgres,
  either degrade with a clear `NucleusError::Unsupported` or map to standard SQL, and assert it.
  Gate behind `#[cfg(feature = "integration")]` / `#[ignore]` so default `cargo test` stays
  hermetic.
- **Tests:** `kv_collections_roundtrip_special_chars` (P0.5),
  `connects_to_tls_required_server` / `verify_full_rejects_bad_hostname` (P0.0),
  `feature_detection_identifies_nucleus`, `plain_postgres_degrades_clearly`.
- **Acceptance:** Collection reads verified against a real engine; TLS-required server reachable;
  plain-Postgres behavior defined and tested; CI job runs the tier on Docker.

### P2.6 — Published benchmarks vs Axum, Actix, Tower, Rocket

- **Files:** extend `rust/bench/`; results into `rust/README.md`.
- **Design:** Reproducible TechEmpower-style harness (plaintext, JSON, routing-heavy, 1KB upload)
  on identical hardware vs **Axum (the bar), Actix-web, raw Tower/hyper, and Rocket**. Include:
  - The **small-body extract micro-bench** from P1.2 (regression guard on streaming-by-default).
  - The **param-allocation hot path** micro-bench: `router.rs` per-param
    `(k.to_string(), v.to_string())` before/after switching to borrowed slices / `Arc<str>`,
    materializing `String` only on extraction.
- **Acceptance:** README publishes reproducible numbers vs all four targets; the param-alloc
  optimization shows a measurable routing improvement with no regressions; the streaming change
  shows no small-body latency regression.

### P2.7 — `MethodRouter` value type + ergonomic test assertions

- **Files:** `rust/crates/neutron/src/router.rs` (`MethodRouter`),
  `rust/crates/neutron/src/testing.rs` (assertions).
- **Design:** First-class composable `MethodRouter` (`route("/x", get(h).post(h2))`) for
  Axum-parity ergonomics; correct 405+`Allow` falls out for free (P0.3). Add `axum-test`-style
  helpers on the test response: `assert_status(code)`, `assert_status_ok()`, `json::<T>()`,
  header/cookie-jar assertions.
- **Tests:** `method_router_composes`, `test_response_assert_helpers`.
- **Acceptance:** `MethodRouter` composes as a value; test response exposes ergonomic assertions.

### P2.8 — Deliberate "won't-do" notes (parity gaps stated, not silent)

- **Rocket `Outcome::Forward`** (fall through to the next matching route on guard failure):
  Neutron has no forward semantics. Decide and **document** either "won't do — single-match
  routing is intentional" or add a guarded-forward mechanism. Silence is worse than a stated
  decision.
- **Actix per-route extractor config** (per-route JSON body limits, per-route error handlers):
  Neutron's body limit is global. Add per-route override (`route(...).body_limit(n)`) or document
  it as a deliberate non-goal. At minimum, allow per-route body-limit override since P1.2's
  streaming limit is enforced in the extractor and is already positioned to take a per-route value.
- **Acceptance:** Each gap has an explicit decision in `rust/README.md` (implemented or
  documented-won't-do).

---

## Every audit issue → phase item (nothing dropped)

| Audit issue | Phase item |
|---|---|
| MEDIUM (a) — Tower bridge buffers body + drops extensions/state (`tower_compat.rs`) | **P1.3** (root-caused by **P1.1** + **P1.2** + **P1.M**) |
| MEDIUM (b) — no streaming request bodies, always buffers (`app.rs:313/677/1006`; `http3_server.rs`) | **P1.2** |
| MEDIUM (c) — 10-layer order unenforced + wrong example | **P1.4** + **P0.6** |
| MEDIUM (d) — `/health` mismatch; only `/healthz`+`/readyz` | **P0.4** |
| MEDIUM (e) — 2 unnecessary `unsafe impl Send/Sync` (`handler.rs:531-532`) | **P0.1** |
| LOW — `router.rs:602` `resolve()` can panic via `.expect()` | **P0.2** |
| LOW — full-workspace test/clippy not exhaustively run | **Quality gates** |
| LOW — no published Axum/Actix benchmarks | **P2.6** |
| Strengths to preserve (matchit, extractors, HTTP/3, features, thread-per-core, RFC 7807) | Preserved throughout; HTTP/3 extended in **P1.2/P1.9**, RFC 7807 extended in **P2.1** |

## Critique-derived additions (beyond the original audit)

| Finding | Phase item |
|---|---|
| Plaintext-only DB client (`pool.rs:182` `NoTls`) — contradicts "any PG client works" | **P0.0** (TLS via rustls + `sslmode`) |
| Two parallel middleware abstractions (`MiddlewareTrait` vs Tower) | **P1.M** (one model; `from_fn` sugar) |
| `FromRef`/`Router<S>` named but undesigned; would not compile | **P1.6** (full trait + derive + merge rules) |
| WS/SSE may regress under Service + streaming-body rework | **P1.9** |
| No tuple/status-override `IntoResponse` (Axum's terse-handler core) | **P1.8** |
| Server-side KV array return is a cross-crate engine change | **N-1** (separately tracked) |
| `OnceLock` resolve design over-engineered | **P0.2** uses simple design (B) |
| `unsafe` test was green-by-vacuity | **P0.1** uses an instantiated `const _` assertion |
| OpenAPI path-divergence asserted without a repro | **P2.2** (prove with a failing test or drop) |
| P1.2 mis-ordered before P1.5 | Reordered in the dependency chain |
| Only one of three body-collect sites cited | **P1.2** enumerates `:313/:677/:1006`, grep-enforced |
| No small-body regression gate for streaming | **P1.2** micro-bench + **P2.6** |
| Graceful-shutdown conformance had no mechanism | **P2.4** controllable-slow-handler harness |
| HTTP/2 & /3 acceptance missing on most P1 items | Every P1 acceptance now reads "HTTP/1, /2, /3" |
| `poll_ready` backpressure claim hollow | **P1.M** propagates readiness through the Tower stack |
| Actix per-route config / Rocket forward unaddressed | **P2.8** (implement or document won't-do) |

## Cross-cutting systemic items (mapped)

| # | Systemic issue | Item |
|---|---|---|
| 1 | 10-layer order enforced nowhere; examples wrong | **P1.4** + **P0.6**, asserted in **P2.4**; one model via **P1.M** |
| 2 | KV `,`/`=` split corruption | **P0.5** (client decode in one place), engine side **N-1**, verified in **P2.5** |
| 3 | Plain-Postgres path weak / untrue "any client works" | **P0.0** (TLS) + **P2.5** (feature detect + degrade + test) |
| 4 | DB correctness mocked | **P2.5** (testcontainers tier) |
| 5 | `/health` wrong shape | **P0.4**, asserted in **P2.4** |

---

## Quality gates (must pass clean for 10/10)

- `cargo build --workspace --all-features` **and** per-feature-matrix builds in isolation:
  `--no-default-features`, `--features json`, `--features form`, `--features tls`,
  `--features tower-compat`, `--features http3`, `--features openapi`, `--features validation`.
- `cargo test --workspace` (hermetic) green; `cargo test --workspace --features integration` green
  on a Docker-enabled CI runner (P2.5), including TLS and KV special-char cases.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` clean.
- `cargo fmt --check`, `cargo doc --no-deps` warning-free, `trybuild` UI tests (P1.6, P1.7) green.
- `cargo-geiger` shows **zero** `unsafe` in `neutron` core except documented, justified blocks
  (P0.1 removes the dead ones).
- **Protocol-parity gate:** the conformance suite (P2.4) and the keystone tests (P1.1/P1.5/P1.3)
  run on **HTTP/1, /2, and /3**.
- **Benchmarks (P2.6):** reproducible numbers vs **Axum, Actix-web, Tower/hyper, Rocket** in
  `rust/README.md`; no routing/throughput regression from the refactors; small-body extract
  latency not regressed by streaming-by-default (pre/post P1.2).

---

## Definition of 10/10 for the Neutron Rust framework

- [ ] **`Router` is a `tower::Service`** — composes, nests opaque services, `oneshot` testing;
      verified on HTTP/1, /2, /3 (P1.1).
- [ ] **One middleware model** — `MiddlewareTrait`/`Next` are sugar lowering to `tower::Layer`;
      user `async fn` middleware and tower-http compose; backpressure propagates (P1.M).
- [ ] **Extractors are async with typed rejections**; every rejection is
      `application/problem+json` (P1.5, P2.1).
- [ ] **Compile-time-checked state** — `Router<S>` + `FromRef` + `#[derive(FromRef)]`; missing
      state is a build error; merge/nest state-coherence rules enforced (P1.6).
- [ ] **Streaming request bodies by default**, O(1) per-request memory, body limit enforced
      mid-stream, no `.collect()` in any of the three dispatch sites, reaching HTTP/3 (P1.2).
- [ ] **Tower-native and lossless** — layers see full state + extensions, no buffering, tower-http
      inherited (P1.3).
- [ ] **Canonical `default_stack()` (real `tower::Layer`s) enforces the contract order**, proven
      by an entry/exit order test; every example uses it (P1.4, P0.6).
- [ ] **`#[debug_handler]` explains handler-bound failures** with trybuild snapshots (P1.7).
- [ ] **Response ergonomics at the bar** — tuple/status-override `IntoResponse`, `Result<T, E>`
      (P1.8).
- [ ] **WS upgrade + SSE survive the rework** with round-trip tests (P1.9).
- [ ] **`GET /health` returns exactly `{status, nucleus, version}`**; `/healthz`+`/readyz` remain
      (P0.4).
- [ ] **Every built-in failure is RFC 7807** with `instance` set (404/405+`Allow`/413/timeout/
      panic/extractor) (P0.3, P2.1).
- [ ] **DB connection is TLS-capable** — rustls + `sslmode`; a TLS-required server is reachable
      (P0.0).
- [ ] **KV collection reads round-trip arbitrary bytes** (`,`, `=`, newline, unicode), verified
      against a real engine (P0.5, P2.5).
- [ ] **"Any Postgres client works" is true and tested** — TLS + feature detection + defined
      plain-PG degradation (P0.0, P2.5).
- [ ] **OpenAPI derives from `serde` types** (P2.2).
- [ ] **`ValidatedJson<T>` yields 422 with field errors** (P2.3).
- [ ] **Contract conformance suite green** across HTTP/1, /2, /3 and shared as the cross-SDK spec
      (P2.4).
- [ ] **Zero unnecessary `unsafe`; `resolve()` never panics** (P0.1, P0.2).
- [ ] **Published, reproducible benchmarks vs Axum, Actix, Tower, Rocket**; strengths preserved and
      measured; no small-body regression (P2.6).
- [ ] **Parity gaps decided, not silent** — Rocket forward / Actix per-route config implemented or
      documented as won't-do (P2.8).
- [ ] **All quality gates green** across the full feature matrix.

**Preserved as-is (already at/above the bar):** matchit radix routing; thread-per-core +
SO_REUSEPORT; array-indexed method dispatch (`MethodMap`); SmallVec params; bodyless-request fast
path; HTTP/3 (quinn+h3); `Body::Stream` response streaming + SSE; `Optional`/`TypedHeader`;
granular Cargo features; dual `TestClient`/`TestServer`; clean RFC 7807 `AppError`.

---

## Residual risks (call out before starting)

1. **`Router<S>` (P1.6) is invasive.** It re-types the entire router surface and the handler
   bound. Budget it as a standalone, single-focus PR with trybuild coverage; do not bundle it with
   P1.2/P1.3 or the diff becomes unreviewable.
2. **HTTP/3 dispatch is a separate loop** (`http3_server.rs`). Every P1 change must be re-verified
   there, or the h3 path silently diverges from h1/h2. The protocol-parity gate exists to catch
   this, but it is the most likely place for incoherence to re-enter.
3. **N-1 (engine-side KV array return) is cross-crate.** Keep the Rust client correct against
   `jsonb` (P0.5) regardless of when N-1 lands; never make the framework PR depend on an engine
   change.
4. **TLS defaults (P0.0).** `sslmode=prefer` as default preserves local dev but means a
   misconfigured prod could silently run plaintext. Document loudly; consider `require` as the
   default for non-localhost hosts.
5. **Streaming-by-default latency (P1.2).** Frame-by-frame limit counting can regress the small-JSON
   common case. The micro-bench gate is mandatory, not optional.
