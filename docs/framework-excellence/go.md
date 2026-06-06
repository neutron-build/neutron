# Neutron Go → 10/10: Implementation Scaffold

> Standalone, phase-by-phase engineering plan to take the Neutron Go framework
> from a strong-but-broken-at-the-edges state to genuinely best-in-class.
> Every citation below was verified against the tree at the time of writing.
> Module: `github.com/neutron-dev/neutron-go`, `go 1.24.0` (so
> `http.ResponseController`, Go 1.22+ method-pattern `ServeMux`, and generics
> are all available).

---

## 0. Grounding corrections (read first)

Two claims from the prior review rounds were **wrong** and are corrected here so
nobody builds on a false premise:

1. **`go/examples/crud-api/main.go` DOES exist.** So do `go/examples/rag-search/`
   and `go/examples/realtime-chat/`. The "every example wires it wrong" thesis is
   **grounded and true**: `examples/crud-api/main.go:64-72` calls
   `neutron.New(WithMiddleware(Logger(logger), Recover(), RequestID(), CORS(...)))`
   — RequestID is registered **after** Logger (so `request_id` is empty in every
   log line, because `Logger` reads `RequestIDFromContext` before RequestID runs),
   Recover is inside Logger, and there is no RateLimit/Auth/Timeout/OTel. Fix the
   examples; do **not** claim they're missing.

2. **`ServeMux.Handler(req)` does NOT let you distinguish 404 from 405** with Go
   1.22 method patterns. For a method mismatch, std ServeMux returns its own
   internal 405 handler (with a populated `Allow` header) and a **non-empty**
   pattern; for an unknown path it returns the `NotFoundHandler` with an empty
   pattern. You cannot key the 404-vs-405 decision on `pattern == ""`. The P0.3
   design below is rebuilt around this reality: **own the match against a
   precomputed method table, and run misses through the middleware chain.**

3. **`go/neutronrealtime/nucleus_stream.go` exists** (37 LOC) and was previously
   unmentioned. It does not assert `http.Flusher`/`http.Hijacker` itself — it
   forwards through the SSE `send` closure — so it has **no independent streaming
   bug**, but it is coupled to the P0.1 SSE fix and must be covered by P0.1's
   through-stack test. Verified; no separate phase needed.

---

## 1. Framing

**Current score: 6.5/10.** `go build ./...`, `go vet ./...`, and
`go test ./... -race` pass. The architecture is genuinely ahead of Gin/Echo/Chi:
typed generic `Register[In, Out]` handlers with auto-binding and auto-OpenAPI 3.1
put it in the same class as **Huma**, the one true peer. But: three correctness
bugs break streaming, the realtime hub, and the framework's own RFC 7807 contract
*behind its own middleware*; the 10-layer contract order is enforced nowhere (and
every example wires it wrong); binding does per-request reflection; the Nucleus
data path stringifies every column, losing PG type fidelity and disabling
prepared statements; and DB correctness is mocked, never integration-tested.

**Target: 10/10.** Beat Huma on its home turf (typed-in/typed-out + OpenAPI),
match Chi/std on streaming/middleware correctness and Encore on local DX — and
ship the artifact no SDK has: a canonical, test-enforced contract-order
middleware stack applied to *every* response (including 404/405), plus a real
cross-engine (Nucleus + stock Postgres) integration tier and generated typed
clients that make "one contract across 8 SDKs" testable rather than marketing.

**Thesis (how it beats the leader).** Huma is router-agnostic and
database-agnostic. Neutron Go is *vertically integrated*: same typed handler
pipeline **plus** a first-class multi-model data layer (Nucleus), a
contract-enforced middleware order, SKIP-LOCKED jobs, tiered cache, and
OAuth2/WebAuthn/CSRF/RBAC built in. To win we must (1) be at least as correct as
Huma on the HTTP edges (streaming, errors, binding, content negotiation, schema
richness) and (2) make the integrated data layer demonstrably type-faithful and
tested against real engines. Huma cannot claim the second — it has no database.
That is the moat, **but only after P1.5/P1.6 land correctly.** The honest current
gap to the leaders: Huma's content negotiation + `$ref`/union schema richness, and
Encore's static analysis + generated clients. Those are now in scope (P1.3, P2.5)
rather than hand-waved.

**Quality bar for every item:** `go build ./...`, `go vet ./...`, and
`go test ./... -race` stay green; `golangci-lint run` (or `go vet` + `staticcheck`)
clean.

---

## Phase order (revised — dependencies made explicit)

```
P0.1  streaming wrappers        (independent)
P0.2  hub send/close race       (independent)
P1.4  method table + route-mw   (PREREQ for P0.3)  ← pulled earlier
P0.3  problem+json 404/405      (depends on P1.4 + middleware-chain dispatch)
P0.4  jobs status errors        (independent)
P1.1  DefaultStack + enforce    (depends on P0.3 for through-chain misses)
P1.2  bindplan + Resolver       (independent; feeds P1.3)
P1.3  validation→OpenAPI + $ref (depends on P1.2 metadata)
P1.5  native pgx (no dual path) (independent)
P1.6  KV array encoding + IT    (independent; IT tier hosts P0.4/P1.5 tests)
P1.7  test harness + health     (depends on P1.1 builtins move)
P2.x  differentiation
```

The critical reorder vs. the draft: **P1.4 (method table) now precedes P0.3**,
because P0.3 cannot be built on `mux.Handler()` pattern-reading. And every miss
path is dispatched **through the middleware chain**, closing a correctness gap the
draft missed entirely.

---

## P0 — Correctness bugs (break real usage today)

### P0.1 — Response-writer wrappers corrupt/break streaming

**Files:** `go/neutron/middleware.go` (statusWriter:254-262, gzipWriter:265-272,
eager `Content-Encoding` at :225, `Content-Length` del at :226),
`go/neutronrealtime/sse.go:12`, `go/neutronrealtime/websocket.go`,
`go/neutronrealtime/nucleus_stream.go` (coupled).

**The headline bug is the eager `Content-Encoding`, not the missing `Unwrap`.**
`Compress` sets `w.Header().Set("Content-Encoding", "gzip")` at middleware.go:225
*before the downstream handler runs* and wraps the writer in a gzip stream. When
the handler is SSE (`Content-Type: text/event-stream`), the result is a gzipped,
buffered, never-flushing stream — the stream is corrupted/hung even if you fix
flushing. Two independent defects compound it:

- `statusWriter` (254) and `gzipWriter` (265) embed `http.ResponseWriter` but
  implement neither `Flush()`, `Hijack()`, nor `Unwrap()`. `sse.go:12`'s
  `flusher, ok := w.(http.Flusher)` **fails** the moment SSE runs behind `Logger`
  (wraps in `statusWriter`) or `Compress` (wraps in `gzipWriter`) → 500
  "streaming not supported". WebSocket `http.Hijacker` assertions break
  identically.
- `Compress` never sets `Vary: Accept-Encoding` (cache-poisoning risk, audit LOW).

**Design — three parts, in priority order:**

**(A) Lazy `Content-Encoding` + content-type sniff (the real fix).** The
`gzipWriter` must defer the compress decision to first `Write`/`WriteHeader`. If,
by then, the downstream set `Content-Type: text/event-stream`, or
`Content-Encoding` is already present, or `Content-Type` is in a known-incompressible
set (images/video/already-gzipped), **pass through uncompressed** and never touch
`Content-Length`. Only when compression is committed do we set
`Content-Encoding: gzip` and delete `Content-Length`. Remove the eager
`Set`/`Del` at middleware.go:225-226.

```go
type gzipWriter struct {
    http.ResponseWriter
    level       int
    gz          *gzip.Writer  // lazily created
    decided     bool
    compressing bool
    wroteHeader bool
}

func (w *gzipWriter) decide() {
    if w.decided { return }
    w.decided = true
    ct := w.Header().Get("Content-Type")
    if w.Header().Get("Content-Encoding") != "" ||
        strings.HasPrefix(ct, "text/event-stream") ||
        isIncompressible(ct) {
        w.compressing = false
        return
    }
    w.compressing = true
    w.Header().Set("Content-Encoding", "gzip")
    w.Header().Del("Content-Length")  // length is unknown post-compress
    gz, err := gzip.NewWriterLevel(w.ResponseWriter, w.level)
    if err != nil { w.compressing = false; w.Header().Del("Content-Encoding"); return }
    w.gz = gz
}

func (w *gzipWriter) WriteHeader(code int) {
    if w.wroteHeader { return }   // idempotency guard (also add to statusWriter)
    w.wroteHeader = true
    w.decide()
    w.ResponseWriter.WriteHeader(code)
}
func (w *gzipWriter) Write(b []byte) (int, error) {
    if !w.wroteHeader { w.WriteHeader(http.StatusOK) }  // Write implies 200
    if w.compressing { return w.gz.Write(b) }
    return w.ResponseWriter.Write(b)
}
```

**(B) `http.ResponseController` cascading unwrap.** Every wrapper gets `Unwrap()`;
gzip also forwards `Flush()` (flush gzip THEN the underlying writer). SSE/WS stop
type-asserting the raw writer and use `http.NewResponseController(w)`.

```go
func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *gzipWriter)   Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *gzipWriter) Flush() error {
    if w.compressing && w.gz != nil {
        if err := w.gz.Flush(); err != nil { return err }
    }
    return http.NewResponseController(w.ResponseWriter).Flush()
}
```

```go
// sse.go — replace w.(http.Flusher)
rc := http.NewResponseController(w)
w.Header().Set("Content-Type", "text/event-stream")
w.Header().Set("Cache-Control", "no-cache")
w.Header().Set("Connection", "keep-alive")
w.WriteHeader(http.StatusOK)
if err := rc.Flush(); err != nil {
    http.Error(w, "streaming unsupported", http.StatusInternalServerError); return
}
send := func(event string, data []byte) error {
    // write "event: ...\ndata: ...\n\n"
    return rc.Flush()   // walks statusWriter -> gzipWriter -> base
}
```

```go
// websocket.go — conn, buf, err := http.NewResponseController(w).Hijack()
```

**(C) `Compress` cleanup.** Always add `Vary: Accept-Encoding`; only wrap when the
client advertises gzip.

```go
func Compress(level int) Middleware {
  return func(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
      w.Header().Add("Vary", "Accept-Encoding")
      if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") { next.ServeHTTP(w, r); return }
      gw := &gzipWriter{ResponseWriter: w, level: level}
      defer func() { if gw.gz != nil { _ = gw.gz.Close() } }()
      next.ServeHTTP(gw, r)
    })
  }
}
```

**Tests** (`middleware_test.go`, `sse_test.go`):
- `TestSSEThroughFullStack`: SSE behind `Chain(RequestID, Logger, Compress(...))`;
  assert events flush+arrive, `Content-Type: text/event-stream`, **no**
  `Content-Encoding: gzip`, and `X-Request-ID` present.
- `TestSSENoBuffering`: write one event, assert the recorder/pipe observes bytes
  **before** the handler returns (timing assertion; use an `io.Pipe`-backed
  writer + flusher and a deadline). Proves no buffering behind Compress.
- `TestResponseControllerUnwrap`: wrap a recorder-flusher in
  `statusWriter`→`gzipWriter`; assert `NewResponseController(top).Flush()` reaches
  the bottom.
- `TestCompressVaryHeader`: `Vary: Accept-Encoding` set even with no
  `Accept-Encoding`.
- `TestCompressGzipRoundTrip`: large compressible body → client decodes; no
  `Content-Length` mismatch (length stripped).
- `TestWriteHeaderIdempotent`: double `WriteHeader` writes once.

**Acceptance.** SSE and WebSocket behave identically with/without
Logger/Compress/RequestID. No gzip on event streams. No buffering. `Vary` set.
`Content-Length` correctly stripped on compress. `go test ./... -race` green.

---

### P0.2 — Send-on-closed-channel race in realtime hub

**File:** `go/neutronrealtime/hub.go` (`close(conn.Send)` at :63; sends at :115,
:133; `Register` `h.mu.Lock()` at :41; `Broadcast` RLock at :100).

**Problem (grounded).** `Unregister` closes `conn.Send` under `h.mu`. `Broadcast`
/`BroadcastAll` copy the conn slice under `RLock`, release, then `c.Send <- msg`.
A concurrent `Unregister` can close between copy and send → **panic: send on
closed channel.** The writer goroutine ranges over `conn.Send`, so closing is how
it terminates — you can't just stop closing.

**Design.** Per-conn mutex + `closed` flag; gate sends and serialize close so
send and close never race (both hold `c.mu`). Preserve existing drop-on-full
semantics. Add slow-consumer eviction.

```go
type Conn struct {
    ID       string
    Send     chan []byte
    rooms    map[string]bool
    mu       sync.Mutex
    closed   bool
    drops    int          // consecutive full-buffer drops
    onClose  func(*Conn)  // hub calls Unregister to evict
}

func (c *Conn) trySend(msg []byte) bool {
    c.mu.Lock()
    if c.closed { c.mu.Unlock(); return false }
    select {
    case c.Send <- msg:
        c.drops = 0; c.mu.Unlock(); return true
    default:
        c.drops++
        slow := c.drops >= slowConsumerThreshold  // e.g. 64
        c.mu.Unlock()
        if slow && c.onClose != nil { c.onClose(c) }  // evict dead/slow client
        return false
    }
}

func (c *Conn) close() {  // called by Unregister, holding nothing else
    c.mu.Lock(); defer c.mu.Unlock()
    if c.closed { return }
    c.closed = true
    close(c.Send)
}
```

`Broadcast`/`BroadcastAll` call `c.trySend`; `Unregister` calls `conn.close()`
instead of bare `close(conn.Send)`.

**Tests** (`hub_test.go`):
- `TestHubConcurrentBroadcastUnregister`: N broadcasters + M (un)registers on the
  same conns for ~1s; no panic under `-race`.
- `TestHubConcurrentRegisterBroadcast`: concurrent `Register` (hub.go:41,
  `h.mu.Lock`) while `Broadcast` holds `RLock` (hub.go:100) — covers the
  register+broadcast surface, not just unregister+broadcast.
- `TestSlowConsumerEvicted`: a conn that never drains → after threshold drops it's
  unregistered and its `Send` closed exactly once.

**Acceptance.** `go test ./neutronrealtime/... -race -count=5` green; no
send-on-closed panic on either the unregister **or** register path; slow consumers
evicted.

---

### P0.3 — Plain-text 404/405 violate RFC 7807, and misses escape middleware

**Files:** `go/neutron/router.go` (bare `r.mux.ServeHTTP` at :123, no
NotFound/MethodNotAllowed hook), `go/neutron/error.go`, `go/neutron/app.go`.
**Depends on P1.4** (method table).

**Two problems, both grounded:**

1. `Router.ServeHTTP` (router.go:123) delegates straight to `http.ServeMux`, whose
   built-in 404/405 emit `text/plain` — the framework that mandates
   `application/problem+json` everywhere violates its own contract on the two most
   common misses, with no `Allow` synthesis on 405.
2. **Middleware is applied per-handler** (`applyMiddleware(handler, r.middleware)`
   at router.go:95/107) — *not* at the router root. So any 404/405 generated in
   `ServeHTTP` runs with **zero** middleware: no `X-Request-ID`, no log line, no
   CORS headers (a CORS preflight `OPTIONS` to an unknown path fails), no OTel
   span. Huma/Chi run NotFound/MethodNotAllowed *inside* the chain. The draft
   missed this; it is a real correctness bug.

**Design.** Do **not** try to read `mux.Handler()`'s mind. Own the match.

- P1.4 builds a precomputed `map[string][]string` of `path-template → methods`.
- `ServeHTTP` first tries the mux. To detect a miss without re-running it, register
  a **root catch-all** `mux.Handle("/", missHandler)` — but that only fires when
  no more-specific pattern matches, and it carries no method context. Cleaner and
  deterministic: keep a **router-level fallback wrapped in the middleware chain.**

```go
// Build once (P1.1): the same []Middleware DefaultStack produces.
func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
    h, pattern := r.mux.Handler(req)
    if pattern != "" {            // real hit (handlers already carry their mw)
        h.ServeHTTP(w, req); return
    }
    // miss: classify via our own table, dispatch THROUGH the root middleware chain
    r.missChain.ServeHTTP(w, req)   // missChain = applyMiddleware(missHandler, r.rootMiddleware)
}

func (r *Router) missHandler() http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
        if allow := r.methods.allowed(req.URL.Path); len(allow) > 0 {
            if req.Method == http.MethodOptions {       // CORS preflight to known path
                w.Header().Set("Allow", strings.Join(allow, ", "))
                w.WriteHeader(http.StatusNoContent); return
            }
            w.Header().Set("Allow", strings.Join(allow, ", "))
            WriteError(w, req, ErrMethodNotAllowed(req.Method, allow)); return
        }
        WriteError(w, req, ErrNotFound("No route matches "+req.URL.Path))
    })
}
```

> Note on `mux.Handler` + method patterns: when the path is known but the method
> isn't, std ServeMux returns its own 405 handler with **non-empty** pattern. To
> keep classification in our hands and guarantee problem+json, we register routes
> on the mux **without** the method prefix where we need full control — OR we keep
> method patterns and accept that std handles the 405 status/`Allow`, then wrap to
> rewrite the body to problem+json. **Chosen approach:** keep method patterns
> (std gives us correct `Allow` for free), and have the miss path consult our
> table only for the *body*. Concretely: detect std's 405 by capturing its
> response via a buffering writer in the fallback; if status==405, re-emit as
> problem+json preserving `Allow`. This leans into std (per the std-1.22 critique)
> instead of duplicating route matching, and the P1.4 table becomes a
> verification/OpenAPI aid rather than the primary 405 source. Pick **one** at
> implementation time and delete the other path — do not ship both.

Add to `error.go`:
```go
func ErrMethodNotAllowed(method string, allow []string) *AppError {
    e := newAppError(http.StatusMethodNotAllowed, "method-not-allowed",
        "Method Not Allowed", method+" is not allowed on this resource")
    e.Meta = map[string]any{"allow": allow}
    return e
}
```

Also route `Timeout`'s deadline path through `WriteError` (504 problem+json when
ctx deadline exceeded and nothing written yet); `Recover` already uses `WriteError`.

**Tests** (`router_test.go`):
- `TestNotFoundIsProblemJSON`: 404, `application/problem+json`, body has
  `type/title/status`.
- `TestMethodNotAllowed`: `GET /x` registered, `POST /x` → 405, `Allow: GET`,
  problem+json.
- `TestMissCarriesMiddleware`: **mandatory** — 404 response carries
  `X-Request-ID` and CORS headers, and a log line was emitted. Proves misses run
  through the chain.
- `TestPreflightToUnknownMethodPath`: `OPTIONS /x` where only `GET /x` exists → 204
  + `Allow: GET` (preflight succeeds).
- `TestTimeout504ProblemJSON`: slow handler → 504 problem+json.

**Acceptance.** 100% of framework responses (404, 405, 500, 504, validation) are
`application/problem+json`; 405 carries `Allow`; **all misses traverse the
middleware chain** (RequestID/Logger/CORS/OTel). No `text/plain` from the
framework.

---

### P0.4 — Jobs swallow status-update errors

**File:** `go/neutronjobs/queue.go` (dropped `Exec` returns at :198, :208, :212).

**Problem (grounded).** `executeJob` ignores the `Exec` return on all three status
transitions (completed/retry/failed). A failed completion-update silently leaves a
job `running` forever — never re-picked, never retried.

**Design.** Capture + log every `Exec` error; add a `reclaimStuck` sweep on the
poll loop that resets `running` jobs whose `updated_at` is older than a lease back
to `pending`.

```go
if _, err := q.client.SQL().Exec(ctx, completeSQL, id); err != nil {
    q.logger.Error("job complete update failed", "id", id, "error", err)
}
```
```go
func (q *Queue) reclaimStuck(ctx context.Context, lease time.Duration) (int64, error) {
    // UPDATE _neutron_jobs SET status='pending', updated_at=NOW()
    // WHERE status='running' AND updated_at < NOW() - $1::interval
}
```

**Tests** (`queue_test.go`, integration tier P1.6): `TestReclaimStuckJobs` — mark a
job `running` with stale `updated_at`, run reclaim, assert it becomes `pending` and
re-runs.

**Acceptance.** No status `Exec` error dropped; stuck `running` jobs self-heal.

---

## P1 — Fundamentals to match (and exceed) the leader

### P1.1 — Canonical contract-order middleware stack + enforcement test

**Files:** new `go/neutron/stack.go`, `go/neutron/app.go`,
`go/examples/crud-api/main.go:64-72` (and `rag-search`, `realtime-chat`).

**Problem (grounded).** `FRAMEWORK_CONTRACT.md` mandates RequestID → Logging →
Recovery → CORS → Compression → RateLimit → Auth → Timeout → OpenTelemetry.
`Chain`/`applyMiddleware` compose correctly but **nothing enforces this order**,
and `examples/crud-api/main.go:69-72` wires `Logger, Recover, RequestID, CORS` —
RequestID *after* Logger (empty `request_id` in every log line), Recover inside
Logger, no RateLimit/Auth/Timeout/OTel. All three example apps and every sibling
SDK share this gap.

**Design.** One constructor returns the slice in exact contract order; `New()`
applies it by default; the **same slice is the `rootMiddleware` used for the miss
chain in P0.3**, so 404/405 are guaranteed to traverse the canonical order too.

```go
// stack.go
type StackOptions struct {
    Logger      *slog.Logger
    CORS        CORSOptions
    Compress    int                 // gzip level; 0 disables
    RateLimit   *RateLimitOptions
    Auth        Middleware          // nil = skip layer
    Timeout     time.Duration
    OTel        OTelOptions
    ServiceName string
}

// DefaultStack returns middleware in FRAMEWORK_CONTRACT order. A layer is
// skipped only when its option is zero/nil. This is the canonical reference.
func DefaultStack(o StackOptions) []Middleware {
    var s []Middleware
    s = append(s, RequestID())                                   // 1
    s = append(s, Logger(orDefault(o.Logger)))                   // 2
    s = append(s, Recover())                                     // 3
    if !o.CORS.isZero()   { s = append(s, CORS(o.CORS)) }        // 4
    if o.Compress > 0     { s = append(s, Compress(o.Compress)) }// 5
    if o.RateLimit != nil { s = append(s, RateLimit(o.RateLimit.RPS, o.RateLimit.Burst)) } // 6
    if o.Auth != nil      { s = append(s, o.Auth) }              // 7
    if o.Timeout > 0      { s = append(s, Timeout(o.Timeout)) }  // 8
    s = append(s, OTel(o.OTel))                                  // 9
    return s
}
```

Add `neutron.WithDefaultStack(StackOptions)`; `New()` applies `DefaultStack` when
no middleware is supplied (overridable via `WithMiddleware`). **Rewrite all three
examples** to consume it. RequestID-before-Logger makes `request_id` populate.

**Tests** (`stack_test.go`):
- `TestDefaultStackOrder`: each middleware appends its layer name to a `[]string`
  in context; a probe handler asserts the exact sequence
  `[requestid, logger, recover, cors, compress, ratelimit, auth, timeout, otel]`.
- `TestRequestIDPrecedesLogger`: capture a log record; assert non-empty
  `request_id`.
- `TestMissUsesDefaultStack`: a 404 carries the full layer sequence (ties P0.3 to
  P1.1).

**Acceptance.** `DefaultStack` exists, `New()` applies it, all examples consume it,
the order test passes, and misses traverse the same order. No example can wire it
wrong because the example *is* `DefaultStack`.

---

### P1.2 — Precomputed bind plan + Resolver + ErrorHandler hook

**Files:** `go/neutron/handler.go` (per-request `reflect.New`/`NumField` walk at
:49, :141-201; `setFieldValue` :204-230), new `go/neutron/bindplan.go`,
`go/neutron/resolver.go`, `go/neutron/errorhandler.go`.

**Problem (grounded).** `Register`'s closure does `reflect.New(inType).Elem()` and
a full tag-walk **on every request**. Huma precomputes this once at startup.
`setFieldValue` handles only scalars + comma-split `[]string`; no `cookie`,
`default`, `time.Time`, `encoding.TextUnmarshaler`, streaming body, or resolver
hook. And `WriteError` is hard-coded — apps can't customize problem+json
(`trace_id`, `instance`, prod redaction), which Echo (`HTTPErrorHandler`) and Huma
(`huma.NewError`) both expose.

**Design.**

**(A) Per-type bind plan, built once, cached.**
```go
// bindplan.go
type bindSource uint8
const ( srcBody bindSource = iota; srcPath; srcQuery; srcHeader; srcCookie; srcForm; srcFile )

type fieldBind struct {
    index    []int            // FieldByIndex (embedding-safe)
    source   bindSource
    key      string
    kind     reflect.Kind
    required bool
    explode  bool             // repeat-key vs comma-list for slices
    def      string           // default tag
    setter   func(reflect.Value, string) error  // resolved once (TextUnmarshaler, time.Time, scalars)
}
type bindPlan struct {
    fields       []fieldBind
    hasBody      bool
    bodyIndex    []int
    bodyIsReader bool          // io.Reader/io.ReadCloser body → stream, skip JSON decode
}
var planCache sync.Map        // map[reflect.Type]*bindPlan
func buildBindPlan(t reflect.Type) *bindPlan
```
`Register` builds/looks up the plan once and closes over it; per request
`rv := reflect.New(t).Elem(); applyPlan(plan, rv, req)` — **zero `reflect.Type`
introspection on the hot path.**

**(B) Resolver (Huma's killer DX feature).**
```go
// resolver.go
type Resolver interface { Resolve(ctx context.Context) []ValidationError }
```
After binding + `Validate`, if `input` (or `*input`) implements `Resolver`, call
it and merge into one **exhaustive 422** (binding + validation + resolver errors
together).

**(C) Configurable error handler.**
```go
// errorhandler.go
type ErrorHandler func(w http.ResponseWriter, r *http.Request, err error)
func WithErrorHandler(h ErrorHandler) Option   // default = WriteError
```
Lets apps add `trace_id`/`instance`, redact internal messages in prod, or change
the media type. `WriteError` becomes the default implementation, not the only one.

**(D) Field-setter extensions.** `cookie` tag, `default:"..."`, `explode`,
`time.Time` (RFC3339), `encoding.TextUnmarshaler`. **BREAKING (version-gate):** the
default for query slices changes from comma-split to **repeat-key**
(`?id=1&id=2`); comma-split only when `explode:"false"` is explicit. This kills the
comma-corruption family but is an API contract change — gate behind a major version
/ opt-in flag and document loudly.

**(E) Streaming uploads.** If the body field is `io.Reader`/`io.ReadCloser`, hand
`req.Body` through and skip JSON decode; pair with per-route `WithBodyTimeout`.

**Tests** (`handler_test.go`, new `bindplan_test.go`):
- `TestBindPlanCachedOncePerType` (instrument a build counter).
- `TestResolverExhaustiveErrors` (2 resolver + 1 validation → single 422 with all 3).
- `TestCookieAndDefaultBinding`, `TestExplodeQuerySlice` (`?id=1&id=2`→`[1,2]`),
  `TestTimeFieldBinding`, `TestTextUnmarshalerBinding`, `TestStreamingBody`.
- `TestCustomErrorHandler` (override adds `trace_id`).
- `BenchmarkBindHotPath` vs baseline (expect material alloc reduction).

**Acceptance.** Zero `reflect.StructField` iteration on the request hot path;
Resolver supported with exhaustive errors; cookie/default/explode/time/TextUnmarshaler
bind; error handler is overridable; benchmark shows fewer allocs/op than baseline.
Breaking slice-default change is version-gated and documented.

---

### P1.3 — Unify validation → OpenAPI (full rule set + `$ref` + examples + unions)

**File:** `go/neutron/openapi.go` (`addValidationConstraints` :401-449 — maps only
`min`/`max`/`oneof`/`email`; CDN Swagger at :461).

**Problem (grounded).** Generated docs silently under-describe the runtime
contract: `pattern`, `len`, `uuid`, `url`, `gte/gt/lte/lt`, `e164`, `datetime`
never reach the schema; `min`/`max` semantics are guessed by Go kind (string→length
else numeric), mismatching go-playground (`min` on a slice = length). And the
schema is **flat-inlined** — nested structs are duplicated rather than `$ref`'d, so
large APIs produce megabyte specs. No examples, no union/`oneOf` support. This is
where Huma is genuinely richer; close it or concede the crown.

**Design.**

**(A) Full go-playground → JSON Schema mapping, kind-correct.**

| validate rule | JSON Schema |
|---|---|
| `min=N` string / number / slice·array·map | `minLength` / `minimum` / `minItems` |
| `max=N` (by kind) | `maxLength` / `maximum` / `maxItems` |
| `gte/gt/lte/lt=N` | `minimum`(+`exclusiveMinimum`) / `maximum`(+`exclusiveMaximum`) |
| `len=N` string | `minLength`+`maxLength` |
| `oneof=a b c` | `enum` |
| `email`/`uuid`/`uri`/`url`/`e164`/`datetime`/`ipv4`/`hostname` | `format` |
| `pattern=...` (custom tag or regexp) | `pattern` |

Reuse the **bind-plan type metadata from P1.2** so there is exactly **one
reflection pass per type** for binding, validation, and schema.

**(B) `$ref` component reuse.** Emit each named struct once into
`components.schemas` and `$ref` it everywhere — required for large APIs. Add a
`schemaRegistry` keyed by `reflect.Type`.

**(C) Examples + unions.** Honor an `example:"..."` tag and an `Examples()` method;
support `oneOf` discriminated unions via an interface marker (`OneOf() []any`) for
polymorphic request/response bodies.

**(D) Auto-`security`.** Track a bool on `routeRecord` when a group/route carries
an auth middleware; attach `security` to those operations.

**(E) Self-hosted docs UI.** Replace the CDN Swagger (openapi.go:461) with pinned
self-hosted assets; default to **Scalar** (lighter to embed than Stoplight).

**(F) Route-pattern accessor for OTel (Chi parity).** Add
`RoutePattern(ctx) string` exposing the matched template (e.g. `GET /users/{id}`),
and make the OTel layer name spans with it — otherwise layer-9 OTel produces
high-cardinality garbage span names (`GET /users/123`).

**Tests** (`openapi_test.go`):
- `TestSchemaCoversAllValidateRules` (every rule → matching keyword/value).
- `TestMinSemanticsByKind` (`min=2`: string→`minLength`, int→`minimum`,
  `[]T`→`minItems`).
- `TestSchemaRefReuse` (nested struct appears once in `components.schemas`,
  `$ref`'d at use sites; no inlined duplicates).
- `TestSchemaExamplesAndOneOf`.
- `TestSecurityAutoAttached`.
- `TestRoutePatternForOTel` (span name == template, not concrete path).

**Acceptance.** Every runtime validation rule appears in the schema with correct
kind semantics; nested types are `$ref`-deduped; examples + unions supported;
security auto-attached; docs self-hosted; OTel spans use the route template. One
reflection pass per type.

---

### P1.4 — Route group consistency + precomputed method table (PREREQ for P0.3)

**File:** `go/neutron/router.go` (`Mount` :84-89, `Static` :136-139, `StaticFS`
:144-147 all call `r.mux.Handle` directly, **bypassing
`applyMiddleware(r.middleware)`**).

**Problem (grounded).** `Mount`, `Static`, `StaticFS` register raw on the mux, so a
static asset or mounted sub-handler silently **escapes group middleware** (CORS,
auth, etc.) — verified at the cited lines, where `Handle`/`register` (:95/:107) do
wrap and these three do not. And P0.3 needs a method table.

**Design.** Route all three through `applyMiddleware(handler, r.middleware)` like
`Handle`/`register`. Build the method table in `register`:
`map[string][]string` (path-template → methods), exposed via
`r.methods.allowed(path)` for P0.3. Keep it as a **verification/OpenAPI aid**; if
P0.3 chooses the "lean on std 405" body-rewrite approach, the table backs the
`/openapi.json` allowed-methods and a startup consistency check rather than the
primary 405 source. Do **not** maintain two parallel matchers.

**Tests** (`router_test.go`):
- `TestStaticHonorsGroupMiddleware` (header-injecting mw appears on a `Static`
  response).
- `TestMountHonorsGroupMiddleware` (same for `Mount`).
- `TestMethodTableMatchesRegistered` (table covers every registered route).

**Acceptance.** No route type bypasses group middleware; method table backs correct
405s / OpenAPI without duplicating the router.

---

### P1.5 — Native pgx scanning + PgError mapping (NO permanent dual path)

**Files:** `go/nucleus/sql.go` (`scanRow` scans every column as `*string`
:145-221; `QueryExecModeSimpleProtocol` forced at :36, :68, :95),
`go/nucleus/client.go`.

**Problem (grounded).** `scanRow` scans **every column as `*string`** then
re-parses — explicitly to dodge "Nucleus pgwire may send binary indicators with
text data." This loses fidelity for `numeric`, `jsonb`, arrays, `bytea`, `uuid`,
`timestamptz`, and forces `SimpleProtocol` everywhere, **defeating prepared
statements**. On stock Postgres it is strictly worse and partly wrong — "any
Postgres client works" is asserted by the very code that proves it doesn't.

**Design — reject the draft's permanent runtime-probe dual path.** A cached
`binaryDecodeOK` bool branching every `Query[T]` between stringify and native
institutionalizes a known-wrong path forever. Instead:

1. **Use native pgx unconditionally**: `pgx.CollectRows(rows, pgx.RowToStructByNameLax[T])`
   with the **default extended protocol** (OID-aware decoders, NULL via
   pointers/`sql.Null*`). Delete `QueryExecModeSimpleProtocol` from sql.go:36/68/95.
2. **File the Nucleus pgwire format-code bug** in `nucleus/` as the blocker (the
   real defect is server-side: pgwire emitting binary indicators with text data).
   Track it; fix it in the engine.
3. **Version-gate, don't branch-forever.** If a specific older Nucleus build still
   needs the shim, gate it on a detected **Nucleus version string** (one
   `server_version`-style check at connect), keep the fallback **narrow and
   per-column-type**, and mark it `// DEPRECATED: remove once nucleus >= X.Y` so
   it deletes cleanly. The default, tested path is native pgx on both engines.

```go
func Query[T any](ctx context.Context, sql *SQLModel, query string, args ...any) ([]T, error) {
    rows, err := sql.pool.Query(ctx, query, args...)  // extended protocol
    if err != nil { return nil, mapPgError(err) }
    return pgx.CollectRows(rows, pgx.RowToStructByNameLax[T])
}
```

4. **PgError → AppError mapping** (contract-correct problem+json for DB errors):
   `23505`→409, `23503`→409, `23502`→400, `22P02`→400, `23514`→400, default→500.

**Tests** (`sql_test.go` + integration tier P1.6):
- `TestPgErrorToAppError` (unique violation → 409).
- Integration: round-trip `numeric`, `jsonb`, `text[]`, `timestamptz`, `bytea`,
  `uuid` against **both** Nucleus and stock Postgres; assert type fidelity and that
  prepared statements are used (extended protocol).

**Acceptance.** Native pgx decoding + extended protocol on both engines; type
fidelity for numeric/jsonb/array/timestamptz/bytea/uuid; PgError→AppError; **no
permanent stringify path** (only a version-gated, deletion-marked shim if strictly
required). "Any Postgres client works" is now demonstrated, not contradicted.

---

### P1.6 — KV collection encoding fix + integration tier (with failure injection)

**Files:** `go/nucleus/kv.go` (`strings.Split(raw, ",")` in `LRange` :267,
`HGetAll` :346 [also splits `=`], `SMembers` :400, `ZRange` :448, `ZRangeByScore`
:464), new `go/nucleus/integration_test.go`, CI.

**Problem (grounded).** All five do `strings.Split(raw, ",")` (HGetAll also splits
`=`). **Any value containing a comma — or `=` for hashes — is corrupted.** This is
the ported cross-SDK delimiter bug. And every `*_test.go` mocks the DB, so
correctness is unverified.

**Design.** Fix at the **wire/protocol layer**, not the delimiter. Have these
`KV_*` Nucleus SQL functions return `text[]` (lists/sets) or `jsonb` (hashes)
instead of a comma-joined string; decode with pgx array/JSON scanning:

```go
func (kv *KVModel) LRange(ctx context.Context, key string, start, stop int) ([]string, error) {
    var out []string
    err := kv.pool.QueryRow(ctx, "SELECT KV_LRANGE($1,$2,$3)", key, start, stop).Scan(&out) // text[]
    return out, wrapErr("kv lrange", err)
}
```
`HGetAll` → `jsonb` object → `map[string]string`. Eliminates delimiter ambiguity
entirely; apply once in the wire contract across all SDKs. Coordinate the `KV_*`
return-type change with the Nucleus engine; until shipped, the array/JSON decode is
the correct client-side target.

**Integration tier.** `testcontainers-go` spinning **both** a real Nucleus
container and `postgres:16`, behind `//go:build integration`. Hosts the P0.4 and
P1.5 integration tests too.

```go
//go:build integration

func TestKVCommaValues(t *testing.T) {
    // RPush "a,b,c"; LRange -> []string{"a,b,c"} (one element, not three)
}
func TestHGetAllValueWithEquals(t *testing.T) {
    // HSet field "k" = "a=b=c"; HGetAll -> map{"k":"a=b=c"}
}
```

**Failure injection (critique #15).** testcontainers can pause/kill containers —
add `TestConnDropRetry` (pause Nucleus mid-query, assert retry/timeout surfaces a
clean `AppError`, not a hang) and `TestPoolExhaustionTimeout`. Happy-path
containers don't prove DB-layer correctness where it actually breaks.

CI job: `go test -tags=integration ./nucleus/... -race` against **both** engines.

**Acceptance.** Comma/`=`-containing KV values survive round-trip on a real engine;
14-model smoke + type-fidelity matrix green; failure-injection tests pass;
integration job gates CI; the mocked-only risk retired.

---

### P1.7 — `humatest`-grade harness + health (live/ready split)

**Files:** `go/neutrontest/helpers.go` (extend), `go/neutron/app.go`
(`registerHealthCheck` :187, registered **only in `Run()`** :136).

**Problem (grounded).** `neutrontest` requires a real `httptest.Server` socket and
only does raw HTTP — no typed call, no problem+json decode. Health, openapi, and
docs are registered **only in `Run()`** — tests using `Handler()` directly get
none of them. `TestTimeoutMiddleware` (middleware_test.go:114-128) is
assertion-free. Dead `float32SliceToSQL` (vector.go:315) is confirmed unused.

**Design.**

**(A) Move builtins out of `Run()`.** A `sync.Once`-guarded `registerBuiltins()`
(health + openapi + docs) called by **both** `Handler()` and `Run()`.

**(B) Typed health + live/ready split (k8s/Encore parity).** Keep the
contract-minimal `{status,nucleus,version}` for readiness; add a liveness endpoint
and per-dependency detail.
```go
type healthResponse struct {
    Status  string                 `json:"status"`  // "ok" | "degraded"
    Nucleus bool                   `json:"nucleus"`
    Version string                 `json:"version"`
    Checks  map[string]healthCheck `json:"checks,omitempty"`  // per-dependency
}
type healthCheck struct { Status string `json:"status"`; LatencyMS float64 `json:"latency_ms"` }
```
- `GET /health/live`  → process up (no dependency checks).
- `GET /health/ready` → dependency checks; returns typed `healthResponse`.
- `GET /health`       → alias of `/health/ready` (preserves existing contract).

**(C) Recorder-based harness (no socket).**
```go
type Client struct{ h http.Handler; t *testing.T }
func New(t *testing.T, app *neutron.App) *Client       // app.Handler(), no socket
func (c *Client) Get(path string) *Response
func (c *Client) PostJSON(path string, body any) *Response
type Response struct{ Status int; rec *httptest.ResponseRecorder }
func (r *Response) JSON(out any)
func (r *Response) Problem() neutron.ProblemDetail      // decodes application/problem+json
```
Plus a pure-unit typed caller:
```go
func Call[In, Out any](t *testing.T, h neutron.HandlerFunc[In, Out], in In) (Out, error)
```

**(D) Fix `TestTimeoutMiddleware`** to assert ctx cancellation observed + 504
problem+json written. **Remove dead `float32SliceToSQL`** (vector.go:315).

**Tests** (`neutrontest` self-test, `app_test.go`):
- `TestHealthExactShape` (`Handler()`-only app → `/health/ready` returns
  `{status,nucleus,version}` typed; `/health/live` returns process status).
- `TestBuiltinsViaHandler` (openapi + docs reachable via `Handler()`).
- `TestHarnessRunsFullStack` (`PostJSON` exercises binding+validation+middleware).
- `TestTimeoutMiddleware` (rewritten — 504 problem+json + cancellation).

**Acceptance.** Health/openapi/docs available via `Handler()` (not just `Run()`);
typed health with live/ready split + per-dependency checks; harness runs the full
pipeline without a socket and decodes problem+json; typed `Call`; timeout test
asserts; no dead code.

---

## P2 — Differentiation to beat the leader

### P2.1 — Cron: adopt `robfig/cron/v3` parser + Nucleus distributed dedup

**File:** `go/neutronjobs/cron.go` (`parseCron` :41-71 handles only `@every`,
`*/N` minutes, two hardcoded specials; `Schedule` per-process `time.Ticker`
:21-35).

**Problem (grounded).** `30 9 * * 1-5` is unsupported, and the per-process ticker
is **not distributed-safe**: N replicas each enqueue → N duplicates.

**Design — do NOT hand-roll the parser.** Hand-rolling a 5-field parser + ranges +
`Next()` is real surface area that `robfig/cron/v3` (the de-facto standard, tiny,
battle-tested) already solves. The CLAUDE.md "lean deps" line is a self-imposed
purity that here costs correctness; one well-vetted dependency is the right call.
Use `cron.ParseStandard` for spec parsing + `Next()`.

**Keep the genuinely novel part:** distributed dedup via Nucleus. A
`_neutron_cron_locks` row keyed by `(job_type, fire_minute)` inserted
`ON CONFLICT DO NOTHING` — only the replica that wins the insert enqueues. This is
the actual differentiator; the parser is commodity.

**Tests** (`cron_test.go`): table-driven specs (`30 9 * * 1-5`, `*/15 * * * *`,
`0 0 1 * *`); `TestCronNoDuplicateEnqueueAcrossReplicas` (integration: two queues,
one fire window, exactly one job enqueued).

**Acceptance.** Standard 5-field cron parses/fires (via `robfig/cron/v3`);
multi-replica schedules enqueue exactly once per window via Nucleus
`ON CONFLICT`.

---

### P2.2 — Sharded rate limiter

**File:** `go/neutron/middleware.go` (`RateLimit` global `sync.Mutex` :155;
eviction sweep inside the lock on the insert path :171-177).

**Problem (grounded).** One global mutex over a map for every request, with the
eviction sweep *inside the lock on the hot path*. Contended under load.

**Design.** Shard buckets by IP hash into
`[]struct{ mu sync.Mutex; m map[string]*tokenBucket }` (e.g. 256 shards); move
eviction to a background `time.Ticker` goroutine sweeping shards off the hot path.
Add `RateLimitOptions{ RPS, Burst }` so `DefaultStack` configures it.

**Tests** (`middleware_test.go`): `TestRateLimitSharded` (semantics preserved) +
`BenchmarkRateLimitParallel` (reduced contention vs global-mutex baseline).

**Acceptance.** Same limiting semantics; no global lock on the request path;
eviction off the hot path; benchmark improvement under parallelism.

---

### P2.3 — Content negotiation (CBOR/msgpack) — Huma home-turf parity

**Files:** `go/neutron/respond.go` (response encode path), `go/neutron/openapi.go`.

**Problem.** Neutron is JSON-only. Huma's marshaler registry (`Accept`-driven
CBOR/msgpack/JSON) is a headline feature. The thesis "beat Huma on its home turf"
is false without it.

**Design.** A small marshaler registry keyed by media type; pick the encoder from
`Accept` (default `application/json`). Ship JSON + CBOR (`fxamacker/cbor`) +
optional msgpack. Problem+json responses also negotiate to `application/problem+cbor`
where appropriate. Register the produced media types in the OpenAPI operation.

**Tests** (`respond_test.go`): `TestAcceptCBOR`, `TestAcceptDefaultsJSON`,
`TestProblemNegotiation`.

**Acceptance.** `Accept: application/cbor` returns CBOR; default JSON unchanged;
negotiated media types appear in OpenAPI. (If descoped, **explicitly concede**
content negotiation to Huma in the README — do not silently claim parity.)

---

### P2.4 — Typed SSE registration in OpenAPI

**Files:** `go/neutronrealtime/sse.go`, `go/neutron/openapi.go`.

**Design.** `RegisterSSE[Event](r, pattern, eventTypes, handler)` records a
`text/event-stream` response + per-event schemas so streams appear in
`/openapi.json` (Huma does this).

**Acceptance.** SSE endpoints documented in `/openapi.json` with event schemas.

---

### P2.5 — Generated typed clients from OpenAPI (the real Encore-beating moat)

**Files:** `cli/` (Go), `go/neutron/openapi.go` (source of truth), new generators.

**Problem.** The thesis is "one contract shared across 8 sibling SDKs," but Neutron
ships **no generated client** for any of them — so the claim is untested marketing.
Encore's actual moat is static API extraction + generated typed clients +
infra-from-code; `neutron dev` alone is a thin slice.

**Design.** Generate typed clients **from the framework's own OpenAPI document**
(it already auto-produces 3.1). Start with TS + Go clients:
`neutron gen client --lang ts|go`. Each generated client mirrors the typed
`In`/`Out` contracts. Add a **round-trip contract test**: spin the example server
(P1.6 container), hit it with the generated client, assert typed responses. This is
what makes "8 SDKs, one contract" *testable* rather than aspirational, and is the
genuine differentiator the leaders lack in combination with a database.

**Tests.** `TestGeneratedTSClientRoundTrip`, `TestGeneratedGoClientRoundTrip`
(integration tier): generate → call live server → typed assertions.

**Acceptance.** `neutron gen client` emits compiling TS + Go clients from the live
OpenAPI; round-trip contract tests pass against the example server.

---

### P2.6 — `neutron new` scaffold = the reference implementation; `neutron dev`

**Files:** `cli/` (Go), templates.

**Design.** `neutron new` emits an app that uses `DefaultStack`, a `Resolver`
example, a working SSE route (through the stack), a `neutrontest` test, the
`live`/`ready` health split, and a generated client (P2.5). The happy-path scaffold
*is* the canonical contract reference — no generated app can be wrong. `neutron dev`
(Encore-style) boots app + a Nucleus testcontainer + opens `/docs` with one
command.

**Tests.** `cli` test: `neutron new` output compiles, its generated test passes,
and `DefaultStack` ordering holds in the scaffold.

**Acceptance.** Generated project builds, tests pass, and demonstrates streaming +
resolver + contract stack + typed client out of the box; `neutron dev` boots
app + Nucleus + docs.

---

## Every audit issue → phase mapping (nothing dropped)

| Audit item | Severity | Phase |
|---|---|---|
| (a) RW wrappers don't forward Flusher/Hijacker; eager `Content-Encoding` corrupts SSE (middleware.go:225,254-272; sse.go:12) | HIGH | **P0.1** |
| (b) Send-on-closed-channel race (hub.go:63 vs :115/:133) | HIGH | **P0.2** |
| (b′) Register+Broadcast race surface (hub.go:41 vs :100) | HIGH | **P0.2** |
| (c) Plain-text 404/405 violates RFC 7807 (router.go:123, no hook) | HIGH | **P0.3** |
| (c′) 404/405 escape the middleware chain (per-handler mw at router.go:95/107) | HIGH (unlisted) | **P0.3** |
| SQL scanRow stringifies every column; SimpleProtocol forced (sql.go:36/68/95/145-221) | MED | **P1.5** |
| KV comma/`=` split corruption (kv.go:267,346,400,448,464) | MED | **P1.6** |
| cron can't parse 5-field + not distributed-safe (cron.go:21-71) | MED | **P2.1** |
| 10-layer order not enforced; examples wired wrong (crud-api/main.go:69-72 + 2 more) | MED | **P1.1** |
| Jobs ignore status-update Exec errors (queue.go:198,208,212) | MED | **P0.4** |
| Static/StaticFS/Mount bypass group middleware (router.go:87,89,139,147) | MED (unlisted) | **P1.4** |
| Assertion-free TestTimeoutMiddleware (middleware_test.go:114-128) | LOW | **P1.7** |
| Dead code float32SliceToSQL (vector.go:315) | LOW | **P1.7** |
| Compress lacks Vary header (middleware.go) | LOW | **P0.1** |

**Cross-cutting systemic items:** #1 middleware order → **P1.1**; #2 KV corruption →
**P1.6**; #3 plain-Postgres fidelity → **P1.5**; #4 unverified DB correctness →
**P1.6** (integration tier + failure injection); #5 exact `/health` shape → **P1.7**
(+ live/ready split). **Leader-gap closers added beyond the audit:** content
negotiation (**P2.3**), `$ref`/examples/unions schema richness + `RoutePattern`/OTel
(**P1.3**), configurable `ErrorHandler` (**P1.2**), generated typed clients
(**P2.5**).

---

## Quality gates

- `go build ./...`, `go vet ./...`, `go test ./... -race -count=5` clean every
  phase.
- `go test -tags=integration ./nucleus/... -race` (Nucleus + stock Postgres via
  testcontainers, **plus failure injection**) green in a dedicated CI job.
- **Benchmarks where performance is claimed.** A `bench/` harness with identical
  "hello + JSON echo + path-param + validated body" routes in **Gin, Echo, Chi,
  std net/http, Huma, Encore, and Neutron**, run `go test -bench . -benchmem`.
  Publish ns/op + allocs/op. Specific claims to back:
  - P1.2 bind plan: Neutron allocs/op ≤ Huma, strictly < Neutron-baseline.
  - P2.2 rate limiter: parallel throughput > global-mutex baseline.
  - Routing parity with std ServeMux (we deliberately keep it — document, don't
    out-trie Chi).
- `golangci-lint run` clean (or at minimum `go vet` + `staticcheck`).

---

## Definition of 10/10 for Neutron Go

- [ ] **Streaming works through the framework's own stack.** SSE/WebSocket
  identical with/without Logger/Compress/RequestID; lazy `Content-Encoding`; no
  gzip on event streams; no buffering; all RW wrappers `Unwrap()`; SSE/WS use
  `http.ResponseController`. (P0.1)
- [ ] **No data races.** `-race -count=5` green; hub send/close serialized;
  register+broadcast covered; slow consumers evicted. (P0.2)
- [ ] **100% of framework responses are `application/problem+json`** — 404 (reason),
  405 (`Allow`), 500, 504, validation — **and all misses traverse the middleware
  chain** (RequestID/CORS/log/OTel on a 404). Nothing emits `text/plain`. (P0.3)
- [ ] **Contract middleware order is a real, applied, test-enforced artifact**
  (`DefaultStack` + `TestDefaultStackOrder`); all three examples consume it; misses
  use the same order. (P1.1)
- [ ] **Binding does zero per-request type reflection**;
  path/query/header/cookie/form/body + default/explode/time/TextUnmarshaler;
  `Resolver` hook with exhaustive errors; **configurable `ErrorHandler`**; query
  slice repeat-key default is version-gated as breaking. (P1.2)
- [ ] **OpenAPI schema == runtime validation** for the full go-playground set, with
  kind-correct semantics; **`$ref` component reuse**, examples, `oneOf` unions;
  auto security; `RoutePattern`-named OTel spans; one reflection pass per type.
  (P1.3)
- [ ] **No route type bypasses group middleware** (Static/StaticFS/Mount). (P1.4)
- [ ] **"Any Postgres client works" is true and tested** — native pgx decoding +
  extended protocol on stock PG **and** Nucleus; type fidelity for
  numeric/jsonb/array/timestamptz/bytea/uuid; PgError→AppError; **no permanent
  stringify path** (only a version-gated, deletion-marked shim if required). (P1.5)
- [ ] **KV collections never corrupt commas/equals**; verified on real engines;
  integration tier (Nucleus + stock Postgres) **with failure injection** gates CI.
  (P1.6)
- [ ] **`neutrontest` harness runs the full pipeline without a socket**, decodes
  problem+json, offers typed `Call`; builtins via `Handler()`; typed health with
  **live/ready split** + per-dependency checks; no dead code; timeout test asserts.
  (P1.7)
- [ ] **Jobs are durable** — no swallowed status updates; stuck jobs self-heal;
  real cron via `robfig/cron/v3`; distributed-safe enqueue via Nucleus
  `ON CONFLICT`. (P0.4, P2.1)
- [ ] **Rate limiter scales** — sharded, eviction off hot path, benchmarked. (P2.2)
- [ ] **Content negotiation (CBOR/msgpack)** or an explicit, documented concession
  to Huma. (P2.3)
- [ ] **Typed SSE in OpenAPI.** (P2.4)
- [ ] **Generated typed clients (TS + Go) from OpenAPI**, round-trip-tested against
  the live server — makes "one contract, 8 SDKs" testable. (P2.5)
- [ ] **The generated scaffold is the reference implementation** (DefaultStack +
  Resolver + working SSE + test + client); `neutron dev` boots app + Nucleus +
  docs. (P2.6)
- [ ] **Published benchmarks vs Gin/Echo/Chi/std/Huma/Encore** (ns/op + allocs/op).
- [ ] **Preserved strengths intact:** typed generic `Register[In,Out]` +
  auto-binding + auto-OpenAPI 3.1; RFC 7807; RBAC/OAuth2+PKCE/WebAuthn/CSRF; tiered
  cache; SKIP-LOCKED queue; lean deps (pgx + validator + the two justified
  additions: `robfig/cron/v3`, a CBOR codec); genuine package-level modularity.

---

## Residual risks / honest caveats

- **P0.3's "lean on std 405 + body-rewrite" vs "own the match" must be decided
  once.** Shipping both duplicates routing. The plan flags this; the implementer
  must pick and delete the other.
- **P1.5 depends on a Nucleus engine fix** (pgwire format codes). Until that lands,
  full extended-protocol fidelity on Nucleus is gated on the version check; the
  client work is correct regardless, but the cross-engine acceptance test for
  Nucleus is blocked on the engine ticket.
- **P1.2's query-slice repeat-key default is breaking.** Even version-gated, it will
  surprise users who relied on `?id=1,2`. Communicate in release notes.
- **P2.5 generated clients are a large surface** (per-language generator
  maintenance). Scoping to TS + Go first is deliberate; the other 6 SDK clients are
  follow-on, not blockers for 10/10 of the *Go* framework.
- **Benchmarks against Huma/Encore require pinning their versions**; numbers will
  drift. Treat the bench harness as CI-tracked, not a one-time claim.

**Key files touched:** `go/neutron/middleware.go` (lazy Content-Encoding, Unwrap/
Flush/Vary, sharded RL), `go/neutron/router.go` (method table, static/mount mw,
miss-through-chain dispatch), `go/neutron/stack.go` (new, DefaultStack),
`go/neutron/handler.go` + `bindplan.go` + `resolver.go` + `errorhandler.go` (new),
`go/neutron/openapi.go` (tag unification, `$ref`/examples/unions, RoutePattern/OTel,
Scalar docs, typed SSE), `go/neutron/respond.go` (content negotiation),
`go/neutron/error.go` (ErrMethodNotAllowed), `go/neutron/app.go` (registerBuiltins
in Handler, typed health + live/ready), `go/neutronrealtime/sse.go` +
`websocket.go` (ResponseController) + `hub.go` (close guard + slow-consumer evict) +
`nucleus_stream.go` (covered via SSE), `go/nucleus/sql.go` + `client.go` (native
scan, no dual path, PgError map), `go/nucleus/kv.go` (array/JSON decode),
`go/nucleus/integration_test.go` (new, + failure injection),
`go/neutronjobs/queue.go` (status errors + reclaim) + `cron.go` (`robfig/cron/v3` +
Nucleus dedup), `go/neutrontest/helpers.go` (harness), `go/examples/{crud-api,
rag-search,realtime-chat}/main.go` (consume DefaultStack), `go/nucleus/vector.go`
(delete dead `float32SliceToSQL`), `cli/` (gen client, neutron new, neutron dev).
