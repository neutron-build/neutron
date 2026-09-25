# Neutron cross-SDK contract conformance suite

Proves that every Neutron language SDK implements **identical, observable behavior**
per [`FRAMEWORK_CONTRACT.md`](../FRAMEWORK_CONTRACT.md). A single language-agnostic
runner boots each SDK's canonical "conformance app" on an ephemeral port given
only as `NEUTRON_HOST`/`NEUTRON_PORT`, asserts the contract over HTTP, boots it
again to send it SIGTERM mid-request, and prints a PASS/FAIL matrix.

The suite is **language-agnostic**: it speaks HTTP, sets environment variables
and sends signals, so the same assertions run unchanged against any SDK
regardless of implementation language.

## Quick start

```bash
cd conformance
node runner/run.mjs              # build + boot + test every available SDK
node runner/run.mjs --no-build   # reuse already-built binaries
node runner/run.mjs go rust      # only the named SDK(s)
node runner/run.mjs --base=http://127.0.0.1:8084   # test an already-running server (HTTP dimensions only)
```

The runner exits non-zero if any contract dimension **fails**, or a skip is
unrecorded, expired or stale in `known-skips.json`, so it is CI-usable as-is.

Requires Node ≥ 18 (uses built-in `fetch`/`zlib`). Per-SDK toolchains:
Go (`go`), Rust (`cargo`), Python (`python3` + `starlette pydantic uvicorn`),
TypeScript (built `@neutron-build/core` and `@neutron-build/cli` — run
`pnpm --filter @neutron-build/core --filter @neutron-build/cli build` inside
`typescript/` first; the SDK is reported absent until then), Elixir
(`elixir` + `mix`), Zig (0.15.x pinned by `zig/build.zig.zon` — 0.16 cannot
compile the SDK; the runner prefers `/opt/homebrew/opt/zig@0.15/bin/zig`,
like `live/executors/zig/run.sh`).

## Structure

```
conformance/
  runner/
    run.mjs        # orchestrator: free-port → boot → wait /health → assert → teardown → matrix
    contract.mjs   # language-agnostic HTTP assertions (one fn per contract dimension)
    lifecycle.mjs  # process-level dimensions: boot env (§6) and SIGTERM drain + exit (§8)
    sdks.mjs       # per-SDK boot descriptors (build cmd, start cmd, availability)
    validate-ir.mjs# re-parses FRAMEWORK_CONTRACT.md, fails if contract-ir.json disagrees
  contract-ir.json # machine-readable single source for every constant the runner asserts
  known-skips.json # recorded skips (dimension × SDK) with reason and expiry
  adapters/
    go/conformance-app/      # canonical no-DB Neutron Go app (own go.mod, replace → ../../../../go)
    rust/                    # → rust/crates/neutron/examples/conformance_app.rs (registered example)
    python/conformance_app.py# canonical no-DB Neutron Python app (imports in-repo SDK)
    typescript/              # `neutron-ts start` in this dir (neutron.config.mjs + routes/) — see its README.md
    elixir/conformance_app.exs
    zig/                     # builds the in-repo SDK as a standalone app (build.zig + src/main.zig)
```

Each conformance app is **database-free** (the `nucleus` health field reports the
"unconfigured" state) and wires the same canonical endpoints:

| Endpoint | Purpose |
|----------|---------|
| `GET /health` | contract health shape `{status, nucleus, version}` (§7) |
| `GET /openapi.json` | OpenAPI 3.1 document (§4) |
| `GET /api/items` | 200 list — compression / request-id probe |
| `POST /api/items` | 422 validation error on a bad body (§2 validation) |
| `GET /errors/{bad-request,unauthorized,forbidden,not-found,conflict,rate-limited,internal}` | forced standard §2 errors |
| `GET /slow` | 200 after 1.5s — the request in flight when `shutdown.sigterm` signals the server (§8) |

Each app takes its listen address from the SDK's own `NEUTRON_HOST` /
`NEUTRON_PORT` handling and reads no addressing variable of its own. Until
2026-09-23 every adapter pinned its port with `PORT` (or passed it explicitly),
which is how Go and TypeScript ignoring `NEUTRON_PORT` stayed invisible.

## Contract dimensions asserted

| Dimension | Contract § | What is checked |
|-----------|-----------|-----------------|
| `health.shape` | §7 | `GET /health` is 200 and body has **exactly** keys `{status, nucleus, version}` |
| `health.types` | §7 | `status`/`version` are strings; `nucleus` is one of the tri-state strings `connected\|disconnected\|unconfigured` |
| `error.rfc7807` | §2 | forced errors carry RFC 7807 fields `type`, `title`, `status`, `detail` |
| `error.contenttype` | §2/§4 | error responses use `application/problem+json` |
| `error.codes` | §2 | each standard status maps to the documented `type` suffix + `title` |
| `validation.format` | §2 | a bad body → 422 with RFC 7807 + `errors[]` of `{field, message[, value]}` |
| `feature.detection` | §1 | the `nucleus` field reports dependency health via `/health` (its only HTTP surface) |
| `openapi.present` | §4 | `GET /openapi.json` is served and parses as JSON |
| `openapi.31` | §4 | spec `openapi` field is `3.1.x` |
| `mw.requestid` | §5 | `x-request-id` response header present (RequestID middleware ran) |
| `mw.cors` | §5 | preflight `OPTIONS` with `Origin` yields `Access-Control-Allow-Origin` |
| `mw.compression` | §5 | `Accept-Encoding: gzip` → `Content-Encoding` + `Vary: Accept-Encoding` |
| `config.env` | §6 | booted with **only** `NEUTRON_HOST`/`NEUTRON_PORT` addressing it (`PORT`/`HOST` removed from the environment), the app answers `GET /health` on that port |
| `shutdown.sigterm` | §8 | SIGTERM to the server process while `GET /slow` is in flight: that request completes 200, a new connection is refused (or answered 503), and the process exits 0 within the drain + 5s |

`config.env` and `shutdown.sigterm` are **lifecycle** dimensions
(`runner/lifecycle.mjs`): they own the process rather than talk to one, so
`--base=URL` cannot run them. The server is started directly — a built binary,
the interpreter, or `neutron-ts`'s bin script, never a wrapper such as `go run`
or `npx` that would take the signal in the SDK's place — and the shutdown probe
gets a fresh boot so the only open connections are its own. A connection the
kernel queued but the app never served (reset at exit — the Zig SDK's
sequential accept loop) counts as refused. A lifecycle FAIL recorded in
`known-skips.json` is reported as a known-gap `skip`, held to the same
unrecorded / expired / stale rules.

### A note on §1 (feature detection) and §3 (KV/wire functions)

Feature detection (§1) is a **connection-time SQL probe** (`SELECT VERSION()`), not
an HTTP endpoint; its only HTTP-observable surface is the `nucleus` field of
`/health`, which the suite asserts. The §3 Nucleus SQL functions require a live
Nucleus database and so are out of scope for the HTTP runner — the one §3-adjacent
contract issue (KV comma-split) is documented below (resolved).

## Current PASS/FAIL matrix

Produced by `node runner/run.mjs --strict` on this machine (go1.26.6, cargo
1.98.0, Python 3.14.7, Node 22.23.2, Elixir 1.20.3 / OTP 29, Zig 0.15.2,
2026-09-23).

```
Dimension            | go      | rust    | python  | ts      | elixir  | zig     
---------------------------------------------------------------------------------
health.shape         | PASS    | PASS    | PASS    | PASS    | PASS    | PASS    | 
health.types         | PASS    | PASS    | PASS    | PASS    | PASS    | PASS    | 
error.rfc7807        | PASS    | PASS    | PASS    | PASS    | PASS    | PASS    | 
error.contenttype    | PASS    | PASS    | PASS    | PASS    | PASS    | PASS    | 
error.codes          | PASS    | PASS    | PASS    | PASS    | PASS    | PASS    | 
validation.format    | PASS    | PASS    | PASS    | PASS    | PASS    | PASS    | 
feature.detection    | PASS    | PASS    | PASS    | PASS    | PASS    | PASS    | 
openapi.present      | PASS    | PASS    | PASS    | PASS    | PASS    | PASS    | 
openapi.31           | PASS    | PASS    | PASS    | PASS    | PASS    | PASS    | 
mw.requestid         | PASS    | PASS    | PASS    | PASS    | PASS    | PASS    | 
mw.cors              | PASS    | PASS    | PASS    | PASS    | PASS    | PASS    | 
mw.compression       | PASS    | PASS    | PASS    | PASS    | PASS    | PASS    | 
config.env           | PASS    | PASS    | PASS    | PASS    | PASS    | PASS    | 
shutdown.sigterm     | PASS    | skip    | PASS    | PASS    | PASS    | PASS    | 
---------------------------------------------------------------------------------

[go] 14 pass, 0 fail, 0 skip
[rust] 13 pass, 0 fail, 1 skip
   skip shutdown.sigterm: known gap (known-skips.json) — observed: in-flight /slow was not drained: ECONNRESET; died by signal SIGTERM instead of exiting (SIGTERM not caught)
[python] 14 pass, 0 fail, 0 skip
[ts] 14 pass, 0 fail, 0 skip
[elixir] 14 pass, 0 fail, 0 skip
[zig] 14 pass, 0 fail, 0 skip
```

**SDKs booted in this environment:** all six. The TypeScript SDK is a web/SSR
meta-framework, but since S81 it implements the full contract surface — RFC 7807
errors, typed validation and OpenAPI 3.1 included — and boots headless via
[`adapters/typescript/`](adapters/typescript/) once the package is built. The
Zig adapter landed with S41 (2026-08-21); the SDK changes it required (a real
`x-request-id` response header, `App.run` actually exiting on SIGTERM, and
three never-compiled surfaces fixed) are pinned by tests in `zig/src`. An SDK
whose toolchain or build is missing is auto-skipped (reported `UNAVAILABLE`),
not failed.

---

## Drift findings

Discovering drift is the point of this suite. Each finding below was either found
by the runner (the matrix `FAIL`s) or confirmed by code inspection where the
surface isn't HTTP-reachable. Findings are stated as **confirmed** / **refuted** /
**partially refuted** / **resolved** against the three drifts the original engine
audit flagged, plus two new ones the runner surfaced. Resolved findings stay
recorded so the same drift isn't rediscovered.

### 1. `/health` nucleus-field TYPE drift — CONFIRMED, then RESOLVED by a contract decision (ea703d97)

The original audit read §7 as `nucleus: true|false` (boolean) and flagged Rust and
TS — both emitted strings — as the drift, with Go/Python's boolean held up as
correct. The resolution went the other way: the string carries a state the boolean
cannot (`unconfigured` is not an error, `disconnected` is), so commit `ea703d97`
("feat(contract): /health nucleus is tri-state connectivity across all SDKs",
2026-06-07) made §7 specify

    "nucleus": "connected" | "disconnected" | "unconfigured"

with the semantics *health of the nucleus dependency* (feature detection — is the
DB a Nucleus instance vs plain Postgres — is §1, not `/health`). Go
(`go/neutron/app.go`) and Python (`python/neutron/app.py`) were migrated from the
boolean to the tri-state; Rust (`health.rs` `HealthCheck::contract`) and TS
already conformed; Elixir's boolean was fixed 2026-08-16. The runner asserts the
tri-state via `contract-ir.json` (`health.nucleusStates`), `validate-ir.mjs` keeps
the IR pinned to the prose, and all five SDKs pass `health.types` and
`feature.detection` today. A client reading `/health` can treat `nucleus`
uniformly across SDKs.

### 2. Validation-error format drift (Rust) — RESOLVED (f6527299)

Contract §2 validation format requires RFC 7807 with `type/title/status/detail`
and an `errors[]` array of `{field, message[, value]}`. The Rust SDK's
`impl IntoResponse for ValidationErrors` used to emit status 422 with
`content-type: application/json` and body `{ "error": …, "fields": { … } }` —
none of the RFC 7807 members, and an object where the array belongs.

Fixed in commit `f6527299` (P2.1/P2.3): the impl now delegates to
`AppError::validation_error` (`rust/crates/neutron/src/validate.rs` →
`rust/crates/neutron/src/error.rs`), which serializes
`application/problem+json` with the full RFC 7807 shape plus a populated
`errors[]`. Pinned by the crate's own tests (`error_response_is_problem_json`,
`invalid_json_returns_422`, `validated_json_rejects_with_422_and_field_errors`)
and by the runner's `validation.format` dimension, which passes on every SDK.

### 3. Middleware order "documented but enforced nowhere" — REFUTED as of current code

The original audit said the contract §5 middleware order is documented but enforced
nowhere. Today every server SDK ships a default stack helper:

- **Go** — `go/neutron/middleware.go` `DefaultStack` (RequestID→Logging→Recovery→
  CORS→Compression→RateLimit→Auth→Timeout→OTel).
- **Python** — `python/neutron/middleware.py` `default_stack`, same order.
- **Rust** — `rust/crates/neutron/src/router.rs` `Router::default_stack`, same
  order (Auth omitted by design: there is no universal default — add it at the
  Auth position after calling `default_stack()`). The `rest_api.rs` example still
  hand-wires its stack and carries a stale "P1.4 will replace this" comment —
  an adoption gap in the example, not a missing helper.
- **TypeScript** — wires Hono's built-in request-id/CORS/compression middleware in
  one place (`server/index.ts`) rather than a named `defaultStack`; the §5
  observable layers are identical.

The runner asserts the observable effects (request id, CORS, compression), and all
five booted SDKs pass `mw.requestid`/`mw.cors`/`mw.compression`.

### 4. KV comma-split bug ported across SDKs — RESOLVED (JSON-array returns)

Contract §3.1 functions that return collections (`KV_LRANGE`, `KV_SMEMBERS`,
`KV_ZRANGE`, `KV_HGETALL`, …) used to return comma-separated strings, and every
SDK client parsed them with a naive `split(',')`, corrupting any member or value
containing a literal comma. The engine now returns JSON arrays and every client
parses structured JSON instead of splitting: Go
(`go/nucleus/kv.go`, `json.Unmarshal`), Python (`python/neutron/nucleus/kv.py`,
`json.loads`), Rust (`rust/crates/neutron-nucleusdb/src/models/kv.rs`,
`serde_json::from_str`), TypeScript
(`typescript/packages/neutron-nucleus/src/kv/index.ts`, `JSON.parse`). Not
exercised by the HTTP runner (no Nucleus DB in CI); recorded as resolved by
inspection.

### 5. Config env-var prefix drift (Rust) — RESOLVED

Contract §6 specifies `{PREFIX}_HOST` / `{PREFIX}_PORT`. The Rust `Config::from_env`
used to read bare `HOST` / `PORT`; it now reads `NEUTRON_HOST` / `NEUTRON_PORT`
(`rust/crates/neutron/src/config.rs`, commit `944b54ac`). Since 2026-09-23 the
`config.env` dimension asserts this for every SDK (finding 6).

### 6. §6 configuration and §8 shutdown were asserted nowhere — RESOLVED, one SDK gap recorded

Measured 2026-09-23: the suite was green while Go (`Run("")`) and TypeScript
(`neutron-ts dev/start`) ignored `NEUTRON_PORT`, and Python drained on SIGTERM
but never exited. None of it was visible, for two reasons in the harness:
every adapter pinned its port with a variable of its own (`PORT` for Go,
Python, TS and Elixir), and nothing ever signalled a server and looked at the
result — the runner's teardown sent SIGTERM and SIGKILLed whatever was left
1.5s later. The SDK fixes landed first (Go `ffe4d3a0`, Python `7b6a1205`, TS
`07ae4b2f`); the `config.env` and `shutdown.sigterm` dimensions now pin them.

Each dimension was shown to catch its bug before it was trusted: with Go's
`listenAddr` stubbed back to "use the argument as given", `config.env` FAILs
(and nothing else can be measured, since the app never listens where it was
told); with Python's `app.py`/`openapi.py` checked out at `7b6a1205~1`,
`shutdown.sigterm` FAILs with "still running 6.5s after SIGTERM"; with
`neutron-ts`'s env lookup or `startServer`'s signal handler removed from the
built dist, the TS row FAILs the matching dimension.

Adding them surfaced two more things:

- **Rust does not catch SIGTERM** — a real §8 MUST violation. `Neutron::listen`
  waits on `tokio::signal::ctrl_c()` (SIGINT) only, so SIGTERM kills the
  process mid-request. Recorded in `known-skips.json` with the fix and an
  expiry; the runner fails the entry the day the dimension passes.
- **The Elixir adapter dropped in-flight requests** — an adapter artifact, not an
  SDK gap. It ran `{Neutron, ...}` under a bare `Supervisor.start_link` in the
  script, which `:init.stop/0` kills once the applications are down. The SDK
  documents running it in the host application's tree; the adapter now does,
  and the drain passes.

---

## Extending the suite

- **Add a contract dimension:** add the check to `runner/contract.mjs` and the
  dimension to `contract-ir.json` (which drives the matrix column order) —
  `runner/validate-ir.mjs` must still pass, so FRAMEWORK_CONTRACT.md and the IR
  have to agree first.
- **Add an SDK:** append a descriptor to `runner/sdks.mjs` with `build()`, `cmd()`
  (returns `{command, args[, cwd]}` that starts the SDK's server process itself —
  no wrapper — listening where the SDK's own `NEUTRON_HOST`/`NEUTRON_PORT`
  handling says), and `available()` (returns `null` if bootable, else a reason
  string). An SDK that does not read `NEUTRON_PORT` yet may declare an adapter
  `portEnv`; it is used only after `config.env` has failed, and that failure
  goes in `known-skips.json`. Add a canonical no-DB conformance app under
  `adapters/<lang>/` wiring the endpoints in the table above, `/slow` included.
