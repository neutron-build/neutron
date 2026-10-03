# Neutron Go

Go SDK for the Neutron ecosystem — an HTTP application framework and a Nucleus
database client covering all 14 data models, in one Go module:
`github.com/neutron-build/neutron/go` (Go 1.26+).

```bash
go get github.com/neutron-build/neutron/go
```

## Packages

| Package | What it is |
|---|---|
| `nucleus` | Nucleus client — feature detection, transactions, all 14 data models |
| `neutron` | HTTP app: router, composable middleware, OpenAPI 3.1, RFC 7807 errors |
| `neutronauth` | JWT, OAuth, WebAuthn, sessions, RBAC, API keys ([session and WebAuthn migration](neutronauth/README.md)) |
| `neutroncache` | Tiered / LRU / HTTP-response caching |
| `neutronjobs` | Background job queue and cron |
| `neutronrealtime` | WebSocket hub, SSE, Nucleus stream subscriptions |
| `neutronmcp` | MCP client/server building blocks |
| `neutrontest` | Test helpers |
| `neutroncli` | The `neutron-go` entrypoint (scaffolding) |

## Nucleus client

`nucleus.Connect` opens a pgx pool and detects Nucleus capabilities via
`SELECT VERSION()`. Every data model is a typed handle on the client:

```go
client, err := nucleus.Connect(ctx, "postgres://localhost:5432/mydb")

client.SQL()        // relational queries
client.KV()         // kv := client.KV(); kv.Set(ctx, key, val, nucleus.WithTTL(time.Hour))
client.Vector()     // Insert, Search
client.TimeSeries() // Insert, Last, RangeAvg
// also Document(), Graph(), FTS(), Geo(), Blob(), Streams(),
// Columnar(), Datalog(), CDC(), PubSub()

err = client.WithTx(ctx, nil, func(tx *nucleus.Tx) error {
    // retries serialization failures (40001/25P02) with full-jitter
    // backoff; lock timeouts (55P03) are surfaced, never retried
    return nil
})
```

`WithTx` is the contract's reference retry helper
(`FRAMEWORK_CONTRACT.md` §3.14); its test asserts a `55P03` is attempted
exactly once.

## Migration metadata namespace

The SDK migration APIs infer one persistent metadata schema from `current_schema()`
at the start of each invocation and qualify all history and claim operations,
including transaction writes, adoption, diagnostics and force-unlock. Configure
the connection search path so that its first existing schema is the intended
migration schema. Temporary shadows, a history or claim found in a later search
path schema, and metadata views or unlogged tables are refused before metadata mutation; remove the
shadow or configure the intended schema first. Quoted schema identifiers are
supported. Existing history version columns must have actual builtin
`pg_catalog.int2`, `int4` or `int8` identity; domains and custom types are refused
before claim metadata is created. Canonical CLI text-ID histories still require
`neutron migrate`.

This freezes metadata resolution across pooled connections and migration SQL
using `SET LOCAL search_path`. Supplied up/down SQL retains its own name-resolution
semantics. Connections must target the same database and authorization principal;
this does not pin a session or protect metadata from privileged concurrent DDL.
The row claim, explicit recovery, checksums and adoption rules are unchanged.

Actual persistent catalog identity is required. Published Nucleus 1.2.0 lacks the
required `pg_catalog.to_regclass` capability, so these migration, status,
lock-info and force-unlock APIs refuse that experimental provider path with an
unsupported namespace profile before mutation. PostgreSQL wire compatibility
alone does not admit a migration provider. Other client SQL/model APIs are not
certified or refused by this migration-specific check.

## HTTP framework

Generic typed handlers — input and output types flow into OpenAPI generation
automatically:

```go
app := neutron.New(neutron.WithOpenAPIInfo("My API", "1.0.0"))
r := app.Router()

neutron.Get[neutron.Empty, User](r, "/api/users/:id", func(ctx context.Context, in neutron.Empty) (User, error) {
    return User{ID: 1, Name: "Alice"}, nil
})

_ = app.Run(":8080")
```

`app.Run("")` listens on `NEUTRON_HOST`/`NEUTRON_PORT` (default all interfaces,
port 8080; an invalid port is an error). A non-empty address always wins.

`GET /health`, `GET /openapi.json`, and `GET /docs` are mounted by default;
errors render as RFC 7807 `application/problem+json`; middleware
(`Logger`, `Recover`, rate limiting, and the rest of the contract stack) is
composed with `app.Router().Group(prefix, mw...)` or `WithMiddleware`.

## Testing

```bash
go test ./...
```

447 test functions across 9 packages. CI: `.github/workflows/go.yml` builds,
vets, and tests the whole module on every change to `go/**`.

---

*This file replaced a pre-implementation design document (2026-08-19). That
document described a multi-module layout (`nucleus-go/kv`, `nucleus-go/vector`,
...), an API that was never built (`ParseConfig`, `kv.New`, `CollectRows`), "9
data models", and ended with "Status: Planned — not yet implemented" — for an
SDK that now ships 447 tests and a CI workflow. Found by the S97 claims audit.
The module path is `github.com/neutron-build/neutron/go` (the `neutron-dev`
path never existed as a GitHub org; the design doc used
`github.com/neutron-build/nucleus-go`).*

`Inet` uses the native inet codec and preserves address host bits as well as the
prefix length, for IPv4 and IPv6. `CIDR` uses the distinct native cidr identity;
`ParseCIDR` refuses addresses with host bits instead of silently masking them.
Both expose immutable `netip.Prefix` values, refuse zones and invalid zero values,
and distinguish a nullable pointer's SQL NULL from a valid all-zero address.
`NewPostgresTable` checks the exact inet/cidr OID; these types are also supported
by the distributed column generator. Network arrays are outside this profile.
