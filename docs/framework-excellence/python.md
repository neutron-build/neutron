# Neutron Python → 10/10 Implementation Scaffold

**Status:** Implementation-ready engineering plan. Execute phase by phase.
**Current: 6.5/10. Target: 10/10** — match FastAPI (the bar), be within noise of Litestar, clearly above Django/Starlette, and *win decisively* on the AI/MCP/agent/RAG axis the leaders don't contest.

All file paths are absolute under `/Users/tyler/Documents/Code Projects/Neutron/python/`. Every bug below has been read and confirmed against source at the cited line.

---

## Framing & thesis

Neutron Python is a hand-rolled FastAPI clone built on Starlette `Route` objects. It reimplements FastAPI's hardened internals more shallowly. Routing (`extract_handler_params`), DI (`resolve_handler_params` recursive re-introspection), OpenAPI (`_type_to_schema`), and validation are **three uncoordinated reflection passes that disagree** — that disagreement *is* the gap from 6.5 to 10.

**The one structural move:** introduce a compiled per-handler `HandlerPlan`, built once at registration, that is the *sole* source of truth for (a) parameter binding, (b) the dependency graph, (c) validation, and (d) OpenAPI schema. FastAPI calls this `Dependant`; Litestar calls it `SignatureModel`. Everything else hangs off it.

**Then beat the leaders on two axes:** (1) the AI/MCP/agent/RAG stack as a first-class, hardened framework layer (the genuine differentiator); (2) msgspec/orjson serialization + a precompiled dependency graph that is faster than FastAPI's per-request `inspect`.

**Preserve (already strong, do not rewrite):** RFC 7807 `AppError` (`error.py`), the auth stack (`auth/jwt.py` alg-confusion guard, `auth/csrf.py`, `auth/oauth.py` PKCE, `auth/password.py` argon2), SSE response streaming (`realtime/sse.py`), and the AI stack. **Note:** graceful-shutdown drain logic is preserved, but its *signal-handler mechanism* is wrong and is fixed in **P0.8** — drain stays, signal grabbing goes.

---

## Phase ordering (authoritative — incorporates red-team re-prioritization)

1. **P0** correctness bugs, in this order: P0.8 (signal handlers, architectural, cheap) → P0.1/P0.3/P0.4/P0.5/P0.6 (independent fixes) → **verify Nucleus KV return contract** → P0.2 (gated on that verification) → P0.7 (integration tier).
2. **P1** fundamentals to match FastAPI: P1.1 (`HandlerPlan` spine) → P1.2 (DI rewrite, incl. non-injecting guard deps) → P1.3 (middleware stack, Recovery-outermost resolved) → P1.4–P1.8 → **P1.9 (`BackgroundTasks`)**.
3. **P2** differentiation: P2.1 (serialization + exclude semantics) → P2.2 (precompiled graph + sync-handler limiter) → P2.3 (typed WS/streaming) → P2.4 (pooled httpx + honest extras + multipart dep) → P2.6 (**mypy strict, runs *after* P1.1/P1.2**) → P2.5 (migrations, **demoted**).
4. Quality gates + benchmarks (benchmark gate rewritten around Litestar, not FastAPI's removed slow path).

---

# Phase P0 — Correctness bugs

## P0.8 — Remove app-level signal handlers (architectural; ship first)
**File:** `neutron/app.py:202-221` (install) and `:272-277` (removal).
**Bug (CONFIRMED):** the lifespan wrapper calls `loop.add_signal_handler(SIGTERM/SIGINT, _signal_handler, sig)`. This **fights uvicorn/granian/hypercorn**, which install their *own* SIGTERM/SIGINT handlers and run the lifespan shutdown themselves. The app also grabbing them is redundant at best and causes double-shutdown / handler-clobbering at worst, and silently no-ops under `workers>1` / Gunicorn (handlers live in the master, lifespan in workers). FastAPI/Litestar deliberately do **not** do this.
**Fix:** delete the `_signal_handler`, the `add_signal_handler` loop (`:214-221`), and the `remove_signal_handler` loop (`:272-277`). Drive drain off the lifespan **shutdown phase only** — i.e. the code after `yield` in `lifespan_wrapper` already runs when the server tears the lifespan down on signal. Keep `_shutting_down`/`_inflight`/`_inflight_zero` and set `_shutting_down = True` at the top of the post-`yield` block (already present at `:231`). The drain `wait_for(_inflight_zero, timeout=_drain_timeout)` stays unchanged.
**Tests:** `test_app.py` — start app via `TestClient` context manager (P1.6), trigger lifespan shutdown, assert drain ran and `on_stop` hooks fired; assert no `loop.add_signal_handler` is called (monkeypatch the loop method to a counter == 0). Assert under a simulated `workers>1` path nothing references signals.
**Acceptance:** zero `add_signal_handler` calls in app code; drain driven purely by lifespan shutdown; on_stop + pool close still ordered.

## P0.1 — Plain-Postgres pubsub: `NOTIFY` takes no bind params
**File:** `neutron/nucleus/pubsub.py:58`.
**Bug (CONFIRMED):** `await conn.execute(f"NOTIFY {channel}, $1", message)` — `NOTIFY` is a utility statement; `$1` is a literal token, the payload is never bound. Off-Nucleus pubsub is silently broken, and `{channel}` is interpolated.
**Fix:**
```python
# pubsub.py publish(), else branch:
async with self._pool.acquire() as conn:
    await conn.execute("SELECT pg_notify($1, $2)", channel, message)
return 0
```
`pg_notify(text, text)` is a function and **does** bind; it also removes the channel-interpolation injection vector on the plain path. Keep `_validate_channel` as defense-in-depth.
**P0.1a — 8000-byte payload guard (red-team #15).** Postgres `NOTIFY`/`pg_notify` errors on payloads > 8000 bytes. Add an explicit guard so "byte-identical round-trip" is honest:
```python
encoded = message.encode("utf-8")
if len(encoded) > 7999:  # leave headroom for the trailing NUL
    raise ValueError(f"pubsub payload exceeds 8000-byte NOTIFY limit ({len(encoded)} bytes)")
```
(Document that larger payloads should go through the Nucleus path or a side-channel KV/blob ref.)
**Tests:** `tests/integration/test_pubsub.py` — publish payload `'{"a":1},x'` and a single/double-quote payload; assert exact bytes on a concurrent `listen()`. Unit: assert the emitted SQL string equals `SELECT pg_notify($1, $2)`. Boundary: 7999-byte payload succeeds, 8001-byte raises `ValueError`.
**Acceptance:** payloads with `,`/`'`/`"` round-trip byte-identical on vanilla Postgres; oversize payload raises before hitting the server.

## P0.3 — Scalar/path coercion via `annotation(raw)` / `inner(raw)` is wrong
**File:** `neutron/handler.py:296 (PATH)`, `:360 (QUERY_SCALAR via _unwrap_scalar_type)`.
**Bug (CONFIRMED):** `param.annotation(raw)` and `inner(raw)` mean `bool("false")` is `True`; `datetime`, `UUID`, `Enum`, `Literal`, constrained ints don't coerce.
**Fix:** route every scalar through a Pydantic `TypeAdapter` cached per-type **at registration** in the `HandlerPlan` (P1.1):
```python
from pydantic import TypeAdapter
adapter = TypeAdapter(param.annotation)          # built once at registration
resolved[name] = adapter.validate_strings(raw)   # "true"->True, ISO->datetime, etc.
# on pydantic ValidationError -> raise validation_error(...) (422 problem+json)
```
For path params, also register Starlette convertors (`{id:int}`, custom `uuid`/`datetime`) so the router rejects malformed paths *before* the handler runs.
**Tests:** `test_coercion.py` — `bool` `"false"`→`False`, `datetime` from ISO, `UUID`, `IntEnum`, `Literal["a","b"]`, `Annotated[int, Query(gt=0)]`; invalid → 422 `application/problem+json`.
**Acceptance:** all listed types coerce; `bool("false")` bug gone; the same `TypeAdapter` instance is reused by coercion, validation (P1.1), and OpenAPI (P1.4).

## P0.4 — Workflow `BaseException` unpack crash
**File:** `neutron/ai/workflow.py:134-145`.
**Bug (CONFIRMED):** `asyncio.gather(..., return_exceptions=True)` can return `BaseException` (incl. `CancelledError`); `isinstance(result, Exception)` (`:134`) misses it, then `output, duration = result` (`:141`) unpacks a `CancelledError` → `TypeError`, masking the real error.
**Fix:**
```python
for name, result in zip(ready, results):
    if isinstance(result, BaseException):     # was: Exception
        trace.append(StepResult(name=name, error=str(result), duration_ms=0.0))
        raise result
    output, duration = result
    ...
```
**Tests:** `test_workflow.py` — a step raising `CancelledError` and a step raising `RuntimeError` both propagate cleanly with `trace` populated; no unpack `TypeError`.
**Acceptance:** original exception surfaces; trace recorded.

## P0.5 — WebSocket set mutation during iteration / live-set aliasing
**File:** `neutron/realtime/websocket.py:101-116`.
**Bug (CONFIRMED):** `members = self._rooms.get(room, set())` (`:101`) aliases the **live** room set; the post-loop `_leave_room`/`discard` (`:113-115`) mutates the same object, and a concurrent `broadcast`/`join` can mutate mid-iteration → `RuntimeError: set changed size during iteration`.
**Fix:** snapshot before iterating:
```python
members = list(self._rooms.get(room, set()))
```
**Tests:** `test_realtime.py` — broadcast to a room while a second asyncio task joins/leaves concurrently; assert no `RuntimeError`, correct `sent` count.
**Acceptance:** concurrent broadcast+mutation never raises.

## P0.6 — Jobs `ON CONFLICT` drops `result`/`scheduled_at`; silent except
**File:** `neutron/jobs/queue.py:115-117` (`DO UPDATE SET`), `:133` (bare `except Exception: pass`).
**Bug (CONFIRMED):** `DO UPDATE SET status=$4, attempts=$5, started_at=$8, completed_at=$9, error=$10` omits `result=$11, scheduled_at=$12`; a re-persisted completed job loses its result. The surrounding `try/except Exception: pass` swallows write failures.
**Fix:** add `result=$11, scheduled_at=$12` to the `DO UPDATE SET`. Replace the bare swallow with `logger.exception("job_persist_failed", job_id=job.id)` (or re-raise — silent data loss is unacceptable; at minimum log).
**Tests:** `tests/integration/test_jobs.py` — enqueue → run → re-persist on conflict; assert `result` and `scheduled_at` survive a round-trip read.
**Acceptance:** job result + scheduled_at persisted on conflict update; write failures logged.

## P0.2 — KV collection reads corrupt values containing `,` / `=` (GATED ON CONTRACT VERIFICATION)
**Files:** `neutron/nucleus/kv.py` — `lrange` (`raw.split(",")`), `hgetall` (`pair.split("=",1)`), `smembers`, `zrange`, `zrangebyscore`; plus `pubsub.py:72 (channels)`.
**Bug (CONFIRMED):** every collection read does `raw.split(",")` / `pair.split("=", 1)`, mangling any value containing the delimiter. This is the ported cross-SDK bug.

**RED-TEAM #7 — ORDERING RISK. This is the one P0 where "ship first, no API change" is FALSE.** The proposed fix changes behavior from *wrong-value* to *raise on CSV*. If Nucleus's `KV_*` functions genuinely return CSV today, the client-side raise turns a data-corruption bug into a hard crash on every KV read across all SDKs.

**Mandatory pre-work — verify the Nucleus return contract first:**
1. Read the Rust source of `KV_SMEMBERS` / `KV_HGETALL` / `KV_LRANGE` / `KV_ZRANGE` in `nucleus/` and determine the actual SQL return type (`text[]`, `jsonb`, or `text` CSV).
2. If they already return `text[]`/`jsonb` → implement the typed-array fix below and the CSV-raise is safe (it's a contract assertion that already holds).
3. If they return CSV `text` → **do not ship the raise first.** File a Nucleus-side contract bug to change the return to `text[]`/`jsonb` for all SDKs, land that, *then* land the client fix. Until then, keep the (broken-on-delimiter) split but add a `# KNOWN: Nucleus KV CSV contract bug, tracked in <issue>` marker and an integration test marked `xfail`.

**Fix (once the server returns arrays/JSON) — at the executor boundary, never per-callsite:**
```python
# nucleus/_exec.py
async def fetch_text_array(self, sql: str, *args) -> list[str]:
    val = await self.fetchval(sql, *args)
    if val is None:
        return []
    if isinstance(val, list):            # asyncpg decodes text[] -> list[str]
        return [str(x) for x in val]
    raise NucleusContractError(f"expected text[]/jsonb, got {type(val).__name__}")
```
`lrange`/`smembers`/`zrange`/`zrangebyscore` call `fetch_text_array`; `hgetall` consumes a `jsonb` object via `fetchval` + `json.loads`. The client must **never** string-split.
**Tests:** `tests/integration/test_kv.py` — `sadd("s","a,b")` then `smembers("s") == ["a,b"]`; `hset("h","k","x=y")` then `hgetall("h") == {"k":"x=y"}`; a fabricated CSV return raises `NucleusContractError`.
**Acceptance:** every collection read preserves delimiters; CSV return fails loud; **and** the Nucleus return contract is confirmed/fixed before the raise ships.

## P0.7 — Integration test tier (vanilla Postgres + optional Nucleus)
**New files:** `tests/integration/conftest.py`, `test_sql.py`, `test_kv.py`, `test_pubsub.py`, `test_jobs.py`, `test_capabilities.py`.
**Design:** `testcontainers[postgres]` session-scoped fixture running **vanilla Postgres**, plus an optional Nucleus container gated on a `NUCLEUS_IMAGE` env var. Mark `@pytest.mark.integration`; `pytest -m "not integration"` stays the fast CI default; a dedicated Docker CI job runs the tier.
```python
@pytest.fixture(scope="session")
async def pg_url():
    with PostgresContainer("postgres:16") as pg:
        yield pg.get_connection_url().replace("postgresql+psycopg2", "postgresql")

@pytest.fixture
async def pg_db(pg_url):
    db = await NucleusClient.connect(pg_url); yield db; await db.close()
```
**Acceptance:** SQL `query`/`query_one` round-trip a Pydantic model against **real** vanilla Postgres; `pg_notify` round-trips; `require_nucleus` raises 503 on the plain path for KV. "Any Postgres client works" becomes *tested*, not asserted (currently every data test is mocked).

---

# Phase P1 — Fundamentals to MATCH FastAPI

## P1.1 — The `HandlerPlan` / signature-model compiler (the spine)
**New file:** `neutron/plan.py`. **Touches:** `router.py`, `handler.py`, `openapi.py`.
**Design.** At registration compile each handler into a single immutable plan:
```python
@dataclass(slots=True)
class HandlerPlan:
    handler: Callable
    is_async: bool
    status_code: int
    response_model: type | None
    response_class: type[Response]        # P2.1 / red-team #14 (default ORJSONResponse)
    resolvers: list[ParamResolver]        # ordered closures pulling from Request
    dep_graph: DependencyGraph            # flattened transitive deps (P1.2)
    route_dependencies: list[DepNode]     # non-injecting guards (P1.2 / red-team #4)
    background_param: str | None          # BackgroundTasks slot (P1.9)
    query_model: type[BaseModel] | None   # one model per location
    header_model: type[BaseModel] | None
    cookie_model: type[BaseModel] | None
    body_adapter: TypeAdapter | None
    return_schema: dict                   # OpenAPI return (P1.4)
    parameters_schema: list[dict]         # OpenAPI params (P1.4)
    operation_id: str                     # stable, for client-gen (red-team #17)

class ParamResolver(Protocol):
    async def __call__(self, request: Request, ctx: "RequestContext") -> Any: ...
```
Build **one Pydantic model per location** (query/header/cookie) from the signature and validate the whole location in a single pydantic-core pass, replacing the `if/elif` ladder in `extract_handler_params` and the per-param loop in `resolve_handler_params` (`handler.py:283-364`). **Delete the "fall through → treat as path" fallback** — it is the source of ambiguous binding. All `inspect`/`get_type_hints`/`TypeAdapter` construction moves to registration time.
**Tests:** `test_plan.py` — a handler with path + query-model + body + `Depends` compiles to a stable plan; `resolvers` count matches; after warmup a request makes **zero** `inspect.signature` calls (monkeypatch a counter → 0).
**Acceptance:** binding, DI, validation, and OpenAPI all read from `HandlerPlan`; no runtime reflection on the request path.

## P1.2 — DI rewrite: yield/teardown, single request cache, overrides, layers, guards
**Files:** `neutron/depends.py`, `neutron/plan.py`, `router.py`, `app.py`.
**Bugs being fixed (CONFIRMED):** per-request re-introspection; **diamond double-resolution** — `dep_cache` is created fresh at the top of `resolve_handler_params` (`handler.py:281`) and the recursive sub-dependency resolution (`:404+`) does **not** thread that cache through, so a dep shared by two siblings resolves twice; no `yield` deps; no `dependency_overrides`.
**Design:**
```python
def Depends(dependency: Callable, *, use_cache: bool = True) -> Any: ...
# Also support Annotated[T, Depends(get_db)].

class DependencyGraph:           # flattened + topologically ordered at registration
    nodes: list[DepNode]         # callable, sub-deps, is_generator, use_cache

class RequestContext:
    cache: dict[Callable, Any]            # ONE per request, shared across the diamond
    exit_stack: AsyncExitStack            # yield-dep teardown, reverse order
    overrides: dict[Callable, Callable]   # app.dependency_overrides
```
Resolution walks the precompiled graph once; **one cache per request**, keyed by the resolved callable (post-override), honoring `use_cache`. `yield`/async-gen deps run via `await ctx.exit_stack.enter_async_context(...)` and are exited in reverse in a `finally` **even when the handler raises** (FastAPI semantics). `app.dependency_overrides[get_db] = fake` is consulted before resolving each node.

**Layered dependencies + non-injecting guards (red-team #4).** Support both:
- **Injected** layered deps: `App(dependencies=[...])`, `Router(dependencies=...)`, `group(dependencies=...)` merge parent→child and bind by name into the handler.
- **Route-level guards** (`dependencies=[Depends(verify_token)]` whose *result is discarded* — pure side-effect/auth gate, FastAPI's `dependencies=`; Litestar's `guards`). These run before the handler, share the same `RequestContext` cache, and raise to short-circuit. Stored in `HandlerPlan.route_dependencies`.
**Tests:** `test_depends.py` — diamond graph resolves the shared dep **once** (call counter == 1); `yield` teardown runs on success *and* on handler raising, in reverse order; `use_cache=False` re-resolves; `dependency_overrides` swaps a dep in tests; a layered group dep applies; a route-level guard dep runs and can 401 without injecting a value.
**Acceptance:** diamond double-resolve gone; yield teardown ordered; overrides work; layered injected deps merge; non-injecting guard deps gate.

## P1.3 — Canonical middleware stack + ordering golden test
**Files:** `neutron/middleware.py` (add Recovery + Auth), new `neutron/stack.py`, `app.py`, `router.py:211`.
**Bugs (CONFIRMED):** no canonical stack; contract order enforced nowhere; `group(middleware=...)` is **silently ignored** (`router.py:211` accepts the param and discards it); Recovery and Auth layers don't exist.
**Contract order (outermost → innermost):** RequestID → Logging → **Recovery** → CORS → Compression → RateLimit → **Auth** → Timeout → OTel.

**RED-TEAM #11 — resolve the Recovery-vs-RequestID contradiction.** Starlette already wraps everything in `ServerErrorMiddleware` (truly outermost). Two valid designs; pick **(A)**:
- **(A) Recommended:** make Neutron's RFC-7807 500 handler *be* the `ServerErrorMiddleware` handler — pass it via `Starlette(..., exception_handlers={Exception: recovery_handler})` so it catches errors from *every* app-stack layer including RequestID/Logging. Then "Recovery" is not a stack layer at all; RequestID stays outermost **within the app stack**, and request_id/trace_id are read from `scope["state"]` (set by RequestID) inside the handler — populate `scope["state"]` defaults *before* the stack so the handler still has them even if RequestID itself raised.
- **(B) Alternative:** keep Recovery as an explicit outermost app-stack layer wrapping RequestID; then drop the "RequestID outermost" claim. Document whichever you choose; do not ship both half-done.

**Design:**
```python
# stack.py
def default_stack(*, cors=None, rate_limit=None, auth=None, timeout=30.0,
                  otel="neutron") -> list[_NeutronMiddleware]:
    """The contract layers, outermost first. Recovery handled via ServerErrorMiddleware (design A)."""
    return [RequestIDMiddleware(), LoggingMiddleware(),
            cors or CORSMiddleware(), CompressionMiddleware(),
            rate_limit or RateLimitMiddleware(), auth or _NoAuth(),
            TimeoutMiddleware(timeout), OTelMiddleware(otel)]
```
Add the RFC-7807 **recovery handler** (non-`AppError` → 500 with request_id/trace_id) wired per design A, and a pluggable **`AuthMiddleware`** slot that populates `scope["state"]["user"]`. Default `App` to `default_stack()` when `middleware is None`. Make `group(middleware=...)` actually apply the sub-router's middleware (or raise `NotImplementedError` if deferred — **never silently ignore**).
**Tests:** `test_stack.py` golden test — each layer appends its name to `scope["state"]["_order"]` on entry; boot app, send a request, assert the observed sequence equals the contract order exactly. Assert error bodies (incl. one raised in a middleware *above* the recovery point) carry `x-request-id`/`x-trace-id` and are `application/problem+json`. Assert `group(middleware=[m])` actually runs `m`.
**Acceptance:** `default_stack()` matches contract order; golden test pins it; Recovery + Auth exist; recovery catches middleware-level errors; `group` middleware applies or raises.

## P1.4 — OpenAPI via `TypeAdapter().json_schema()` (delete the bespoke mapper)
**File:** `neutron/openapi.py:259-270` (`_type_to_schema`) — **delete** (every non-primitive currently collapses to `{"type":"string"}` at `:270`).
**Design:** for **every** parameter and return type:
```python
schema = TypeAdapter(t).json_schema(ref_template="#/components/schemas/{model}")
defs = schema.pop("$defs", {})
components_schemas.update(defs)   # hoist nested models
```
This is the *same* `TypeAdapter` used for coercion (P0.3) and the location models (P1.1) — one source of truth. Constraints from `Annotated[int, Query(gt=0, le=100)]` (P1.5) flow through automatically.
**Also add (red-team #17 — a 10/10 spec needs these):**
- Stable `operationId` per route (from `HandlerPlan.operation_id`; default `f"{method}_{route_name}"`) — client generators depend on it.
- Multi-status responses: a `responses=` kwarg per route; auto-document the `ProblemDetail` schema for declared error codes.
- `tags` propagated from router/group; request/response `examples` pulled from Pydantic `model_config`/`Field(examples=...)`.
- Security scheme auto-wiring from the Auth layer (P1.3) so `Depends(require_auth)` emits a `securitySchemes` entry + per-op `security`.
**Tests:** `test_openapi.py` — a handler with `status: OrderStatus(Enum)`, `created: datetime`, `id: UUID`, `Annotated[int, Query(gt=0)]` produces an enum schema, `format: date-time`, `format: uuid`, `exclusiveMinimum: 0` — not `{type:string}`. Assert stable `operationId`, a documented 422/`ProblemDetail`, and a security scheme. Snapshot the full spec.
**Acceptance:** zero `{"type":"string"}` collapses; enums/datetime/uuid/constraints/unions present; `operationId`, multi-status, tags, examples, security all emitted.

## P1.5 — `Annotated` field markers with constraints (kill bespoke `Query[T]`) + multipart dep
**Files:** `neutron/handler.py` (replace `_QueryMarker`/`Query[T]` class-getitem), new `neutron/params.py`, `pyproject.toml`.
**Bugs (CONFIRMED):** `Query[Model]`/`Header[Model]`/`Form[Model]` are bespoke `__class_getitem__` markers, not `Annotated` metadata; no per-field `Query(gt=0)`; header binding does lossy `dict(request.headers)` (`handler.py:335`) — headers are multi-value and case-insensitive; form binding does `dict(await request.form())` (`:367`).
**RED-TEAM #3 — `python-multipart` is NOT declared.** `Form`/`File`/`request.form()` raise at runtime without it. **Add `python-multipart>=0.0.9` to core (or to a `[forms]` extra and import-guard with a clear error).**
**Design:** FastAPI-idiom marker objects carrying validation **and** OpenAPI metadata, all `Annotated`-based:
```python
# params.py
class Query(FieldInfo):   ...   # Annotated[int, Query(gt=0, alias="p", default=1)]
class Path(FieldInfo):    ...
class Header(FieldInfo):   ...  # multi-value -> list[T], case-insensitive (getlist)
class Cookie(FieldInfo):   ...
class Body(FieldInfo):    ...   # Body(embed=True)
class Form(FieldInfo):    ...
class File(FieldInfo):    ...
```
Keep the old `Query[T]` model-binding form working via a deprecation shim mapping to a location model. Multi-value query/headers bind to `list[T]` using `request.query_params.getlist` / `request.headers` case-insensitive multi-access — never `dict()`.
**Tests:** `test_params.py` — `Annotated[int, Query(gt=0)]` rejects `0` with 422; `alias` works; `Header` multi-value → list; header lookup case-insensitive; deprecation shim still binds a model.
**Acceptance:** `Annotated[T, Query/Path/Header/Cookie/Body/Form/File(...)]` drives both validation and schema; multi-value preserved; `python-multipart` declared.

## P1.6 — `TestClient`/`AsyncTestClient` + `dependency_overrides` + real WS transport
**Files:** `neutron/test/__init__.py` (currently async-only over `ASGITransport`), `app.py` (add `self.dependency_overrides: dict = {}`).
**Bug (CONFIRMED):** the existing `TestClient` is an `httpx.AsyncClient(ASGITransport(app))` wrapper — no sync client, no overrides, and **httpx/ASGITransport cannot test WebSockets**.
**RED-TEAM #9 — pin the WS transport.** Sync WS testing needs Starlette's portal-based `TestClient` (which *can* do `websocket_connect`), or `httpx-ws` for the async client. Do not hand-wave.
**Design:**
```python
class TestClient:          # sync; wraps Starlette's TestClient (anyio portal) for WS + lifespan
    def __enter__(self): ...        # enters app lifespan (runs on_start/on_stop)
    def websocket_connect(self, url): ...   # via Starlette portal

class AsyncTestClient:     # httpx.AsyncClient(ASGITransport) + asgi-lifespan LifespanManager
    async def websocket_connect(self, url): ...  # via httpx-ws
```
`app.dependency_overrides[dep] = fake` is consulted in P1.2 resolution and cleared per test. Ship pytest fixtures (`app`, `client`, `db` against the P0.7 testcontainers).
**Tests:** `test_testclient.py` — override `get_db` with a fake; lifespan `on_stop` runs under the context manager; `websocket_connect` echoes a message (proving the transport actually carries WS frames).
**Acceptance:** sync + async clients; overrides; lifespan; **working** websocket testing via a transport that supports it.

## P1.7 — Universal RFC 7807: Starlette 404/405/HTTPException
**Files:** `app.py:180-189` (exception_handlers), `error.py`.
**Bug (CONFIRMED):** raw Starlette 404 returns plain `{"detail":"Not Found"}`, bypassing problem+json; `handle_500` only fires on the literal `500` key and only when `debug=False`; validation `errors[]` drop pydantic `type`/`ctx`.
**Design:** register handlers for `StarletteHTTPException` and the default 404/405 → problem+json; the catch-all `Exception` handler is the recovery handler from P1.3 design A. Pull `request_id`/`trace_id` from `scope["state"]` into every error body + headers. Extend `ValidationErrorDetail` to preserve native pydantic `type` and `ctx`.
**Tests:** `test_error.py` — 404 on an unknown route is `application/problem+json` with `instance` + `request_id`; 405 likewise; a validation error includes `type` and `ctx`.
**Acceptance:** every error path (incl. Starlette 404/405) is RFC 7807 with correlation IDs.

## P1.8 — Real capability probing
**File:** `neutron/nucleus/client.py:184-212` (`_detect_features`).
**Bug (CONFIRMED):** after `is_nucleus = "Nucleus" in version_string`, all 14 `has_*` are set `= is_nucleus`; individual capabilities are never probed. Partial-Nucleus / extension-on-Postgres (e.g. pgvector) is misreported.
**Design:** probe the catalog once after version detection:
```python
rows = await conn.fetch(
    "SELECT proname FROM pg_proc WHERE proname = ANY($1)",
    [["kv_get", "vector_distance", "graph_shortest_path", "ts_insert",
      "doc_get", "fts_search", "geo_distance", "blob_put", "stream_append",
      "col_scan", "datalog_query", "cdc_subscribe", "pubsub_publish"]])
present = {r["proname"] for r in rows}
return Features(is_nucleus=is_nucleus, version=version,
                has_kv="kv_get" in present, has_vector="vector_distance" in present, ...)
```
Plain Postgres with pgvector then correctly reports `has_vector=True`. `GET /health` continues to read `features.is_nucleus`.
**Tests:** `tests/integration/test_capabilities.py` — vanilla PG → all `has_*` False; a fixture installing a single probed function → only that cap True.
**Acceptance:** capability flags reflect the actual catalog, not a boolean fan-out.

## P1.9 — `BackgroundTasks` (table-stakes; blocks parity claim) — NEW
**Files:** `neutron/background.py` (new), `neutron/plan.py`, `neutron/response.py`, `app.py`.
**Bug (CONFIRMED):** zero references to `BackgroundTasks` anywhere. This is core FastAPI/Starlette surface; parity cannot be claimed without it.
**Design:** a `BackgroundTasks` collector injectable as a `ParamResolver` in the `HandlerPlan`:
```python
async def handler(bg: BackgroundTasks):
    bg.add_task(send_email, to=..., body=...)   # runs AFTER the response flushes
```
Wire into the response send via Starlette's `Response(background=...)` mechanism (Starlette already runs `background` after the body is sent). The plan records `background_param`; the resolver creates a fresh `BackgroundTasks` per request and attaches it to the outgoing response.
**RED-TEAM #1 interaction with drain (P0.8).** Background tasks must run *after* the response but their lifetime overlaps the drain window. Increment the in-flight counter (`app._inflight`) for the duration of background-task execution, or they can be killed mid-flight on shutdown. Concretely: wrap the background runner so `_inflight` stays > 0 until tasks complete, and the drain `wait_for(_inflight_zero)` covers them. Document this as a hard constraint.
**Tests:** `test_background.py` — `bg.add_task` runs after the response is received by the client (assert ordering via a shared event); a task still completes if enqueued just before shutdown (drain waits for it); exceptions in a task are logged, not swallowed silently.
**Acceptance:** `BackgroundTasks` works, runs post-response, and is drain-safe.

---

# Phase P2 — Differentiation to BEAT the leaders

## P2.1 — msgspec/orjson serialization + full `response_model` semantics (default)
**Files:** `neutron/response.py`, `neutron/plan.py`, `pyproject.toml`.
**Bugs (CONFIRMED):** responses go `model_dump(mode="json")` → dict → stdlib `json.dumps` (double encode); no orjson; `response_model` **re-validates** every item via `model_validate` (`response.py:34-45`) — the FastAPI footgun, here unconditional.
**Design:** add `ORJSONResponse` (the new default) and an optional `MsgspecResponse` hot path:
```python
class ORJSONResponse(Response):
    media_type = "application/json"
    def render(self, content): return orjson.dumps(content, default=_default)
```
Serialize models **directly** (`model_dump()` objects → orjson; no intermediate `mode="json"` re-encode). Make `response_model` **serialization-only** — never re-validate when the input already is the response type; use `model_dump(...)`/`model_construct`. msgspec `Struct` encoders are an opt-in for genuinely beating FastAPI.
**RED-TEAM #2 — serialization-only is what *enables* the exclude knobs; ship them or you're less capable than FastAPI.** Support `response_model_exclude_unset`, `response_model_exclude_none`, `response_model_exclude_defaults`, `response_model_include/exclude`, and `by_alias` on the route decorator, threaded into the `model_dump(...)` call.
**RED-TEAM #14 — per-route `response_class=`.** Allow overriding the default `ORJSONResponse` per route (HTML, plaintext, streaming) so the orjson default is overridable. Store on `HandlerPlan.response_class`.
**Deps:** add `orjson` to core; `msgspec` to a `[fast]` extra.
**Tests:** `test_response.py` — orjson default; `response_model` does **not** re-validate (monkeypatch `model_validate` counter == 0 on the happy path); `exclude_none`/`by_alias`/`include` honored; datetime/UUID serialize; `response_class=PlainTextResponse` overrides.
**Acceptance:** orjson default; no response re-validation; full exclude/by_alias semantics; per-route response class; benchmark gate (below) passes.

## P2.2 — Precompiled dependency graph + bounded sync-handler offload
**Files:** `plan.py`, `handler.py`, `router.py:94`, `app.py`.
Precompiled graph is delivered by P1.1/P1.2 — the perf acceptance is called out here: after warmup a request does **zero** `inspect.signature`/`get_type_hints`/`extract_handler_params` calls.
**RED-TEAM #6 — bound the sync-handler offload (CONFIRMED `asyncio.to_thread` at `router.py:94`).** Every sync handler spawns onto the default executor (`min(32, cpu+4)` threads) with no app-level limiter → a silent throughput cliff under load. Route sync handlers through an `anyio.to_thread.run_sync` with a shared `anyio.CapacityLimiter` (configurable via `App(sync_thread_limit=...)`), mirroring FastAPI/Starlette. At minimum document the bound; ideally enforce + expose it.
**Tests:** `test_perf_invariants.py` — patch `inspect.signature` to raise after app build; a request still succeeds (no runtime reflection). `test_sync_offload.py` — N concurrent sync handlers respect the capacity limiter (assert max in-flight ≤ limit).
**Acceptance:** zero runtime reflection on the request path; sync-handler concurrency is bounded and configurable.

## P2.3 — Typed router owns WebSockets + streaming request body
**Files:** `router.py` (add `@router.websocket`), `realtime/websocket.py`, `handler.py` (`UploadFile`/`Stream`).
**Bugs (CONFIRMED):** the WebSocket hub is hand-mounted, disconnected from router/DI/OpenAPI; `UploadFile` is hollow (the `FILE` branch at `handler.py:391-396` constructs a bare `UploadFile()` and assigns `.file` — no async `read/seek/size`); `request.form()` is awaited eagerly (`:367, :382`) so large uploads buffer in memory; no `request.stream()`.
**Design:** `@router.websocket("/ws/{room}")` participating in the `HandlerPlan` + DI (path params, `Depends`, guards), emitting an AsyncAPI-style channel entry. Wrap Starlette's `UploadFile` (already a spooled `SpooledTemporaryFile`) exposing async `read/write/seek/size/content_type`. Add a `Body(stream=True)` / `Stream` extractor yielding `request.stream()` for chunked ingest. SSE: add a keep-alive ping interval + `is_disconnected()` cancellation-safety to `sse_response`.
**Tests:** `test_router.py` — `@router.websocket` resolves a path param + `Depends`; `test_streaming.py` — a large upload uses the spooled file (assert it spills to disk / peak memory bounded); SSE keep-alive ping observed; SSE stops cleanly on client disconnect.
**Acceptance:** typed websocket routing with DI; streaming uploads; SSE keep-alive + disconnect-safe.

## P2.4 — Pooled httpx per provider + honest extras + session dirty-flag
**Files:** `neutron/ai/providers.py:100,147,172`, `neutron/auth/session.py`, `neutron/auth/password.py`, `pyproject.toml`.
**Bugs (CONFIRMED):** `httpx.AsyncClient()` is created **per call** (no pooling) — connection setup on every LLM request. The `[ai] = ["httpx"]` extra is a no-op (httpx is already core). `bcrypt` fallback in `auth/password.py` is undeclared. Session "save on every response" (audit LOW).
**Design:**
- One lifespan-scoped `httpx.AsyncClient` per provider instance (created in `__init__`, closed via `app.on_stop`/`aclose`).
- pyproject: make extras honest — either remove the no-op `[ai]` or move `httpx` into `[ai]` and out of core (pick one); declare `bcrypt` under a `[crypto]` extra if it's a runtime fallback. **Also fold in P1.5's `python-multipart` and P2.1's `orjson`/`msgspec[fast]` here so the dependency story is reconciled in one place.**
**RED-TEAM #16 — session dirty flag (give it a test, not a note).** In `auth/session.py`, track a `_dirty` flag; only persist on response when the session actually changed. Add an explicit test.
**Tests:** `test_ai.py` — a provider reuses one client across N calls (assert client identity); `test_session.py` — an unchanged session does **not** write on response; a mutated session does; `test_pyproject.py` (or a CI grep) — no no-op extras.
**Acceptance:** httpx connection reuse; extras honest and reconciled; session writes only when dirty.

## P2.6 — mypy `--strict` to zero (runs AFTER P1.1/P1.2)
**Files:** all files with errors; `pyproject.toml` (enable full strict).
**Bug:** ~131 errors across ~33 files despite a "strict-flavored" config; the typed-DX claim is oversold.
**RED-TEAM #12 — sequencing.** P1.1 deletes/rewrites `handler.py`, which is the bulk of the `Any` churn. **Do not fix the old `handler.py` errors — run P2.6 after P1.1/P1.2 land** so the work isn't thrown away.
**Design:** set `strict = true`; fix to zero. Missing third-party stubs → a *narrow, justified* `# type: ignore[code]`, never a blanket relaxation. If a corner genuinely can't type, relax that one flag *honestly* in pyproject with a comment rather than claiming full strict.
**Acceptance:** `mypy --strict neutron` → 0 errors (or documented, narrow per-flag exceptions).

## P2.5 — Migrations beyond raw SQL (DEMOTED)
**File:** `neutron/nucleus/migrate.py`.
**RED-TEAM #8 — mis-prioritized.** FastAPI ships *no* migration story (people use Alembic); this is differentiation spend on an axis the leaders don't contest. **Keep but demote below P1.9 / P2.1 exclude-semantics.** Do not let it consume P2 oxygen ahead of table-stakes.
**Design (scoped, not over-engineered):** keep raw-SQL as the substrate; add (a) a `Migration` dataclass with `up`/`down`, (b) checksum verification of applied migrations (drift detection), (c) a `--dry-run` plan. Full ORM-diff autogenerate is **out of scope** — Neutron's positioning is "any Postgres client," not an ORM.
**Tests:** integration — apply, verify checksum, detect a tampered migration, roll back via `down`.
**Acceptance:** reversible, drift-detecting migrations.

---

## Smaller leader-grade gaps (red-team Tier 4 — fold into the relevant phase)
- **Typed `app.state` / `request.state`** (red-team #13): expose a typed `State` accessor (Litestar `State`, FastAPI `request.state`) over `scope["state"]`, not just the internal `RequestContext`. Fold into P1.1/P1.6.
- **Per-route `response_class`** (#14): done in P2.1.
- **OpenAPI `operationId`/examples/tags/security** (#17): done in P1.4.
- **`pg_notify` 8000-byte guard** (#15): done in P0.1a.
- **Session dirty flag** (#16): done in P2.4.

---

## Every audit issue → phase mapping (nothing dropped)

| Audit / red-team issue | Phase item |
|---|---|
| ARCH: app-level signal handlers fight the server (app.py:206-221) | **P0.8** (new) |
| HIGH: pubsub `NOTIFY $1` broken (pubsub.py:58) | **P0.1** |
| HIGH: `pg_notify` 8000-byte limit unhandled | **P0.1a** |
| MED: workflow `BaseException` unpack crash (workflow.py:134-141) | **P0.4** |
| MED: mypy ~131 errors / typing oversold | **P2.6** (after P1.1/P1.2) |
| MED: websocket set-mutation-during-iteration (websocket.py:101) | **P0.5** |
| MED: 10-layer middleware order enforced nowhere | **P1.3** |
| LOW: jobs `ON CONFLICT` omits result/scheduled_at + silent except (queue.py:115,133) | **P0.6** |
| LOW: per-call httpx, no pooling (providers.py:100,147,172) | **P2.4** |
| LOW: KV comma/equals split lossy (kv.py lrange/hgetall/smembers/zrange) | **P0.2** (gated on Nucleus contract) |
| LOW: session save on every response (auth/session.py) | **P2.4** (dirty flag + test) |
| LOW: `[ai]` extra no-op, bcrypt fallback undeclared | **P2.4** |
| GAP: DI lacks yield/teardown + overrides + sub-dep caching + guards | **P1.2** |
| GAP: `BackgroundTasks` entirely absent | **P1.9** (new) |
| GAP: OpenAPI scalars collapse to `{type:string}` (openapi.py:270) | **P1.4** |
| GAP: OpenAPI no operationId/examples/tags/security | **P1.4** |
| GAP: no orjson/msgspec fast path; response re-validation (response.py:34-45) | **P2.1** |
| GAP: `response_model` exclude/by_alias semantics missing | **P2.1** |
| GAP: per-route `response_class` missing | **P2.1** |
| GAP: raw-SQL-only migrate | **P2.5** (demoted) |
| GAP: no websocket routing in typed router | **P2.3** |
| GAP: `python-multipart` undeclared (form/file raise at runtime) | **P1.5 / P2.4** |
| Routing: `annotation(raw)`/`inner(raw)` coercion bug (handler.py:296,360) | **P0.3** |
| Routing: `group(middleware=...)` ignored (router.py:211) | **P1.3** |
| Routing: unbounded `asyncio.to_thread` sync offload (router.py:94) | **P2.2** |
| Binding: bespoke `Query[T]`, no `Annotated`/constraints, lossy headers (handler.py:335) | **P1.5** |
| Error: Starlette 404/405 bypass problem+json; lost `type`/`ctx` | **P1.7** |
| Data: capability fan-out (client.py:199-211) | **P1.8** |
| Data: no integration tier (everything mocked) | **P0.7** |
| Perf: per-request dep re-introspection; diamond double-resolve (handler.py:281,404) | **P1.2, P2.2** |
| Streaming request body absent (UploadFile hollow, form eager) | **P2.3** |
| SSE keep-alive / disconnect-safety | **P2.3** |
| Test client: async-only, no WS, no overrides (test/__init__.py) | **P1.6** |
| Recovery-vs-RequestID outermost contradiction | **P1.3** (design A) |
| Typed `app.state`/`request.state` | **P1.1/P1.6** |
| Preserve: AppError, auth, SSE, AI stack | Framing — untouched |

---

## Cross-cutting systemic items
1. **Middleware order enforced nowhere** → **P1.3** `default_stack()` + golden ordering test + the missing Recovery (via ServerErrorMiddleware) and Auth layers + `group` middleware applied.
2. **KV CSV-split corruption (ported bug)** → **P0.2**, fixed at the executor boundary (typed arrays/JSON), never string-split; CSV return raises — **but only after the Nucleus return contract is verified/fixed** (the one P0 that changes behavior).
3. **Plain-Postgres weak/untrue** → **P0.1** (pg_notify), **P1.8** (real probing), **P0.7** (tested against vanilla PG).
4. **DB SQL unverified (mocked)** → **P0.7** testcontainers tier (vanilla PG + optional Nucleus).
5. **`GET /health` contract** → already returns `{status, nucleus, version}` (`app.py:144-157`); **lock it with a contract test** asserting exact keys/types.
6. **Graceful shutdown** → drain logic preserved, signal mechanism replaced (**P0.8**); background tasks made drain-safe (**P1.9**).

---

## Quality gates
- **`pytest -m "not integration"`** green in CI (fast); **`pytest -m integration`** green in a Docker CI job.
- **`mypy --strict neutron`** → 0 errors (P2.6, sequenced after P1.1/P1.2).
- **`ruff check`** + **`ruff format --check`** clean.
- **Benchmarks** — *rewritten per red-team #10; the FastAPI-revalidation microbench is unfalsifiable and removed as the headline:*
  - **Headline gate (the one you might fail):** **JSON-echo throughput within 10% of Litestar** (msgspec-default) — this is the real bar.
  - **Mixed-handler p99:** a realistic handler (path + query-model + body + one `yield` dep) — measure p99 vs FastAPI and Litestar; must beat FastAPI, within noise of Litestar.
  - **DI hot path:** precompiled-graph resolution vs FastAPI `Dependant` (micro) — must not regress.
  - Floors: clearly above Django REST and raw Starlette (sanity).
  - No perf claim ships without a published table. Tools: `oha`/`wrk` against equivalent apps in each framework.

---

## Definition of 10/10 for this framework
- [ ] One `HandlerPlan` compiler is the sole source for binding, DI, validation, and OpenAPI — **zero** runtime reflection on the request path.
- [ ] DI has `yield`/teardown, one request-scoped cache (no diamond double-resolve), `app.dependency_overrides`, layered (app/router/group) injected deps, **and non-injecting route-level guard deps**.
- [ ] `BackgroundTasks` works, runs post-response, and is drain-safe.
- [ ] OpenAPI emits real enums/datetime/UUID/constraints/unions/multi-status + stable `operationId` + examples + tags + auto-wired security — no `{type:string}` collapse.
- [ ] `default_stack()` applies the contract middleware order; a golden test pins it; Recovery (catches middleware-level errors) + Auth exist; `group(middleware=...)` works or raises.
- [ ] Every error (incl. Starlette 404/405) is RFC 7807 with request_id/trace_id.
- [ ] orjson/msgspec default response; `response_model` never re-validates; full exclude/by_alias semantics; per-route `response_class`; benchmark within 10% of Litestar on JSON-echo, beats FastAPI on the mixed-handler p99.
- [ ] Sync-handler offload is bounded by a configurable capacity limiter.
- [ ] `TestClient`/`AsyncTestClient` with lifespan + **working** websocket transport + overrides.
- [ ] App-level signal handlers removed; drain driven purely by lifespan shutdown.
- [ ] Typed router owns websockets (DI-aware); streaming uploads via spooled `UploadFile`; SSE keep-alive + disconnect-safe.
- [ ] Plain-Postgres path correct (`pg_notify` + 8000-byte guard) and **tested**; real capability probing; "any Postgres client works" proven by the integration tier.
- [ ] KV reads never string-split; CSV return fails loud — **after** the Nucleus return contract is verified/fixed.
- [ ] `python-multipart`, `orjson` declared; extras honest; session writes only when dirty.
- [ ] `GET /health` returns exactly `{status, nucleus, version}` (contract test).
- [ ] `mypy --strict` = 0, `ruff` clean, both test tiers green.
- [ ] AI/MCP/agent/RAG stack preserved and hardened (pooled httpx, workflow crash fixed) — the differentiator that makes this the best **AI** framework, not just another FastAPI.

---

## Residual risks (flagged for the implementer)
1. **P0.2 ordering** is the sharpest: the client-side CSV-raise must not land before the Nucleus `KV_*` return type is confirmed/changed, or it converts data corruption into a hard crash across all SDKs. Treat the Nucleus-contract check as a blocking prerequisite.
2. **Recovery-vs-ServerErrorMiddleware** (design A) must be wired so `scope["state"]` defaults exist *before* the stack, or an error in RequestID itself yields an error body with no correlation ID.
3. **Background-task drain coupling** (P1.9 ↔ P0.8): if the in-flight counter doesn't cover background execution, shutdown can kill tasks mid-flight — easy to get subtly wrong.
4. **Benchmark honesty:** the Litestar JSON-echo gate is the one that can fail; resist swapping it back to the trivially-winnable FastAPI-revalidation microbench.
5. **`HandlerPlan` migration surface:** P1.1 rewrites the hottest, most-coupled module (`handler.py`); land it behind the integration tier (P0.7) so behavior is pinned before the rewrite.

---

**Files referenced (all absolute):**
`/Users/tyler/Documents/Code Projects/Neutron/python/neutron/{handler,router,openapi,depends,app,middleware,response,error}.py`,
`.../neutron/nucleus/{pubsub,kv,client,_exec,migrate}.py`,
`.../neutron/jobs/queue.py`, `.../neutron/ai/{providers,workflow}.py`,
`.../neutron/realtime/{websocket,sse}.py`, `.../neutron/auth/{session,password}.py`,
`.../neutron/test/__init__.py`, `.../pyproject.toml`.
**New files:** `.../neutron/{plan,stack,params,background}.py`, `.../tests/integration/*`.
