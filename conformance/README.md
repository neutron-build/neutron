# Neutron cross-SDK contract conformance suite

Proves that every Neutron language SDK implements **identical, observable behavior**
per [`FRAMEWORK_CONTRACT.md`](../FRAMEWORK_CONTRACT.md). A single language-agnostic
runner boots each SDK's canonical "conformance app" on an ephemeral port, asserts
the contract over HTTP, tears the server down, and prints a PASS/FAIL matrix.

The suite is **transport-only**: it speaks HTTP and inspects responses, so the
same assertions run unchanged against any SDK regardless of implementation language.

## Quick start

```bash
cd conformance
node runner/run.mjs              # build + boot + test every available SDK
node runner/run.mjs --no-build   # reuse already-built binaries
node runner/run.mjs go rust      # only the named SDK(s)
node runner/run.mjs --base=http://127.0.0.1:8084   # test an already-running server
```

The runner exits non-zero if any contract dimension **fails** (skips do not fail),
so it is CI-usable as-is.

Requires Node ≥ 18 (uses built-in `fetch`/`zlib`). Per-SDK toolchains:
Go (`go`), Rust (`cargo`), Python (`python3` + `starlette pydantic uvicorn`).

## Structure

```
conformance/
  runner/
    run.mjs        # orchestrator: free-port → boot → wait /health → assert → teardown → matrix
    contract.mjs   # language-agnostic assertions (one fn per contract dimension)
    sdks.mjs       # per-SDK boot descriptors (build cmd, start cmd, availability)
  adapters/
    go/conformance-app/      # canonical no-DB Neutron Go app (own go.mod, replace → ../../../../go)
    rust/                    # → rust/crates/neutron/examples/conformance_app.rs (registered example)
    python/conformance_app.py# canonical no-DB Neutron Python app (imports in-repo SDK)
    typescript/README.md     # document-only: why TS isn't auto-booted + its confirmed drift
```

Each conformance app is **database-free** (the `nucleus` health field reports the
"unconfigured/disconnected/false" state) and wires the same canonical endpoints:

| Endpoint | Purpose |
|----------|---------|
| `GET /health` | contract health shape `{status, nucleus, version}` (§7) |
| `GET /openapi.json` | OpenAPI 3.1 document (§4) |
| `GET /api/items` | 200 list — compression / request-id probe |
| `POST /api/items` | 422 validation error on a bad body (§2 validation) |
| `GET /errors/{bad-request,unauthorized,forbidden,not-found,conflict,rate-limited,internal}` | forced standard §2 errors |

## Contract dimensions asserted

| Dimension | Contract § | What is checked |
|-----------|-----------|-----------------|
| `health.shape` | §7 | `GET /health` is 200 and body has **exactly** keys `{status, nucleus, version}` |
| `health.types` | §7 | `status`/`version` are strings; **`nucleus` is a boolean** (`true\|false`) |
| `error.rfc7807` | §2 | forced errors carry RFC 7807 fields `type`, `title`, `status`, `detail` |
| `error.contenttype` | §2/§4 | error responses use `application/problem+json` |
| `error.codes` | §2 | each standard status maps to the documented `type` suffix + `title` |
| `validation.format` | §2 | a bad body → 422 with RFC 7807 + `errors[]` of `{field, message[, value]}` |
| `feature.detection` | §1 | `nucleus` detection state is exposed via `/health` (its only HTTP surface) |
| `openapi.present` | §4 | `GET /openapi.json` is served and parses as JSON |
| `openapi.31` | §4 | spec `openapi` field is `3.1.x` |
| `mw.requestid` | §5 | `x-request-id` response header present (RequestID middleware ran) |
| `mw.cors` | §5 | preflight `OPTIONS` with `Origin` yields `Access-Control-Allow-Origin` |
| `mw.compression` | §5 | `Accept-Encoding: gzip` → `Content-Encoding` + `Vary: Accept-Encoding` |

### A note on §1 (feature detection) and §3 (KV/wire functions)

Feature detection (§1) is a **connection-time SQL probe** (`SELECT VERSION()`), not
an HTTP endpoint; its only HTTP-observable surface is the `nucleus` field of
`/health`, which the suite asserts. The §3 Nucleus SQL functions require a live
Nucleus database and so are out of scope for the HTTP runner — the one §3-adjacent
contract issue (KV comma-split) is documented below as a static finding.

## Current PASS/FAIL matrix

Produced by `node runner/run.mjs` on this machine (Go 1.26, cargo 1.93, Python 3.14,
Node 26). Deterministic across runs.

```
Dimension            | go      | rust    | python
---------------------------------------------------
health.shape         | PASS    | PASS    | PASS
health.types         | PASS    | FAIL    | PASS
error.rfc7807        | PASS    | PASS    | PASS
error.contenttype    | PASS    | PASS    | PASS
error.codes          | PASS    | PASS    | PASS
validation.format    | PASS    | FAIL    | PASS
feature.detection    | PASS    | PASS    | PASS
openapi.present      | PASS    | PASS    | PASS
openapi.31           | PASS    | PASS    | PASS
mw.requestid         | PASS    | PASS    | PASS
mw.cors              | PASS    | PASS    | PASS
mw.compression       | PASS    | PASS    | PASS
---------------------------------------------------
[go]     12 pass, 0 fail, 0 skip
[rust]   10 pass, 2 fail, 0 skip
[python] 12 pass, 0 fail, 0 skip
```

**SDKs booted in this environment:** Go, Rust, Python (3 of 4).
**Not booted:** TypeScript — it is a web/SSR framework, not a JSON API server; it
needs a network `pnpm install` + build and does not expose `/openapi.json` or
RFC 7807 HTTP errors. See [`adapters/typescript/README.md`](adapters/typescript/README.md).

---

## Drift findings

Discovering drift is the point of this suite. Each finding below was either found
by the runner (the matrix `FAIL`s) or confirmed by code inspection where the
surface isn't HTTP-reachable. Findings are stated as **confirmed** / **refuted** /
**partially refuted** against the three drifts the original engine audit flagged,
plus two new ones the runner surfaced.

### 1. `/health` nucleus-field TYPE drift — CONFIRMED (runner FAIL)

The contract §7 specifies `nucleus: true|false` (boolean). Two SDKs emit a **string**
instead:

- **Rust** — `rust/crates/neutron/src/health.rs:96-103` defines
  `ContractHealthResponse.nucleus: &'static str` and emits `"connected"` /
  `"disconnected"` / `"unconfigured"`. The crate's own test even asserts the string
  shape (`health.rs:501` `assert_eq!(parsed["nucleus"], "unconfigured")`).
- **TypeScript** — `typescript/packages/neutron/src/server/index.ts:351` emits
  `nucleus: "unconfigured"`.

Whereas the contract-faithful **boolean** is emitted by:

- **Go** — `go/neutron/app.go:194-197` (`resp["nucleus"] = …IsNucleus()` / `false`).
- **Python** — `python/neutron/app.py:154-156` (`"nucleus": is_nucleus`, a `bool`).

This is a genuine cross-SDK divergence: a client reading `/health` cannot treat
`nucleus` uniformly across SDKs. The runner's `health.types` dimension fails Rust on
exactly this (TS would fail too if booted). **The original audit's "/health shape
drift" is CONFIRMED**, and now pinpointed to a `boolean` vs `string` type split.

### 2. Validation-error format drift (Rust) — CONFIRMED (runner FAIL)

Contract §2 validation format requires RFC 7807 with `type/title/status/detail` and
an `errors[]` array of `{field, message, value}`. The Rust SDK's validation response
does **not** conform:

- `rust/crates/neutron/src/validate.rs:321-342` — `impl IntoResponse for
  ValidationErrors` emits status 422 but with `content-type: application/json`
  (not `application/problem+json`) and body
  `{ "error": "Validation failed", "fields": { … } }` — it has **none** of
  `type/title/status/detail` and uses a `fields` object instead of an `errors`
  array.

Go (`go/neutron/error.go:76-80,91-105`) and Python
(`python/neutron/error.py:39-53,81-86`) both emit the correct RFC 7807 validation
shape. The runner's `validation.format` dimension fails Rust on exactly this.

### 3. Middleware order "documented but enforced nowhere" — PARTIALLY REFUTED

The original audit said the contract §5 middleware order is documented but enforced
nowhere. As of current code this is **partially refuted**:

- **Enforced via a helper** in **Go** (`go/neutron/middleware.go:50` `DefaultStack`,
  hard-codes RequestID→Logging→Recovery→CORS→Compression→RateLimit→Auth→Timeout→OTel)
  and **Python** (`python/neutron/middleware.py:507` `default_stack`, same order).
- **NOT enforced** in **Rust** or **TypeScript** — neither ships a `default_stack`/
  `defaultStack`; order is hand-wired in examples with a comment that
  acknowledges the gap (`rust/crates/neutron/examples/rest_api.rs:223-227`:
  "P1.4 will replace this hand-wired stack with `default_stack()`"). TS hand-wires
  CORS-before-Compression inline in `server/index.ts:318-343`.

So the order is **observable and correct** where the conformance apps are wired
(all three booted SDKs pass `mw.requestid`/`mw.cors`/`mw.compression`), but it is
only **structurally enforced** in Go and Python. The runner asserts the observable
effects, not internal layering — internal enforcement is a code-structure finding.

### 4. KV comma-split bug ported across SDKs — CONFIRMED (static)

Contract §3.1 has several KV functions return **comma-separated** strings
(`KV_LRANGE`, `KV_SMEMBERS`, `KV_ZRANGE`, `KV_HGETALL`, …). Every SDK's Nucleus
client parses these with a naive `split(',')`, so any member/value containing a
literal comma is corrupted (split into multiple elements) on read:

- **Go** — `go/nucleus/kv.go:267,346,400,448,464`
- **Python** — `python/neutron/nucleus/kv.py:118,157,182,215,226`
- **Rust** — `rust/crates/neutron-nucleus/src/models/kv.rs:215,299,354,409,429`
- **TypeScript** — `typescript/packages/neutron-nucleus/src/kv/index.ts:274,314,344,368,375`

The same pattern also appears in the Document clients (comma-separated ID lists)
and PubSub channel lists. This is a data-fidelity drift identical across all four
clients — **CONFIRMED**. It is not exercised by the HTTP runner (no Nucleus DB in
CI); it requires a live Nucleus instance and a member containing `,` to reproduce.
Fixing it requires a contract-level escaping/encoding decision (e.g. length-prefixed
or JSON-array returns) applied uniformly across the engine + all clients.

### 5. Config env-var prefix drift (Rust) — minor, observed

Contract §6 specifies `{PREFIX}_HOST` / `{PREFIX}_PORT`. The Rust `Config::from_env`
reads **bare** `HOST` / `PORT` (`rust/crates/neutron/src/config.rs:24-28`), with no
framework prefix, while the contract's intent is a prefixed var. Go's example uses an
explicit addr. Not asserted by the runner (boot-time only), recorded for completeness.

---

## Extending the suite

- **Add a contract dimension:** add a check to `runner/contract.mjs` and its id to
  the `DIMENSIONS` array (drives matrix column order).
- **Add an SDK:** append a descriptor to `runner/sdks.mjs` with `build()`, `cmd()`
  (returns `{command, args}` and reads its port from the `portEnv` var), and
  `available()` (returns `null` if bootable, else a reason string). Add a canonical
  no-DB conformance app under `adapters/<lang>/` wiring the endpoints in the table
  above.
