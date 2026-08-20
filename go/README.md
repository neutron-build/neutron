# Neutron Go

Go SDK for the Neutron ecosystem — an HTTP application framework and a Nucleus
database client covering all 14 data models, in one Go module:
`github.com/neutron-dev/neutron-go` (Go 1.24+).

```bash
go get github.com/neutron-dev/neutron-go
```

## Packages

| Package | What it is |
|---|---|
| `nucleus` | Nucleus client — feature detection, transactions, all 14 data models |
| `neutron` | HTTP app: router, composable middleware, OpenAPI 3.1, RFC 7807 errors |
| `neutronauth` | JWT, OAuth, WebAuthn, sessions, RBAC, API keys |
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
The real module path is `github.com/neutron-dev/neutron-go`; the design doc
used `github.com/neutron-build/nucleus-go`.*
