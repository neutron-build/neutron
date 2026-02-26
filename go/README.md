# Neutron Go

Go language SDK for the Neutron ecosystem — Nucleus database client (all 9 data models) and Neutron HTTP server bindings.

## Philosophy

Light core, modular data models. Import only what you use. No codegen, no magic zero-values, no god objects. SQL stays visible. The `Querier` interface makes everything testable without mocks.

## Modules

```
github.com/neutron-build/nucleus-go          — core SQL client (always imported)
github.com/neutron-build/nucleus-go/kv       — Key-Value (optional)
github.com/neutron-build/nucleus-go/vector   — Vector search (optional)
github.com/neutron-build/nucleus-go/graph    — Graph traversal (optional)
github.com/neutron-build/nucleus-go/ts       — Timeseries (optional)
github.com/neutron-build/nucleus-go/doc      — Document store (optional)
github.com/neutron-build/nucleus-go/fts      — Full-text search (optional)
github.com/neutron-build/nucleus-go/geo      — Geo queries (optional)
github.com/neutron-build/nucleus-go/pubsub   — Pub/Sub (optional)
github.com/neutron-build/neutron-go          — HTTP server bindings
```

A pure-SQL service imports only `nucleus-go`. Vector, graph, and timeseries contribute zero binary size to services that don't need them.

## Struct Tag ORM

Two tags, clearly separated:

- `db:"column_name"` — column mapping for query scanning (inherited from pgx/sqlx — already what Go developers expect)
- `nucleus:"column_name,pk,notnull,unique,index,default:now()"` — DDL metadata for schema reflection

```go
type User struct {
    ID        int64     `db:"id"        nucleus:"id,pk"`
    Name      string    `db:"name"      nucleus:"name,notnull"`
    Email     string    `db:"email"     nucleus:"email,notnull,unique"`
    CreatedAt time.Time `db:"created_at" nucleus:"created_at,default:now()"`
}
```

**No magic zero-value treatment.** A zero `bool` stores as false. A zero `int64` stores as 0. `Update` requires explicit column names — no "update non-zero fields only" surprises.

## The Querier Interface

The single most important design decision. Both `*Client` (pool) and `Tx` (transaction) implement it:

```go
type Querier interface {
    Exec(ctx context.Context, sql string, args ...any) (Result, error)
    Query(ctx context.Context, sql string, args ...any) (Rows, error)
    QueryRow(ctx context.Context, sql string, args ...any) Row
}
```

Repository functions accept `Querier`. In tests, pass a `Tx` that rolls back after each test — zero mocking, fully isolated, parallel-safe tests with no cleanup logic.

## SQL Client

```go
cfg := nucleus.ParseConfig("postgres://localhost:5432/mydb")
client, err := nucleus.New(ctx, cfg)

// Type-safe struct scanning
users, err := nucleus.CollectRows[User](
    client.Query(ctx, "SELECT * FROM users WHERE active = $1", true),
)

// Named args
user, err := nucleus.CollectOneRow[User](
    client.Query(ctx, "SELECT * FROM users WHERE id = @id", nucleus.NamedArgs{"id": 42}),
)
```

## Transactions

`WithTx` commits on nil return, rolls back on error or panic. Transactions are always explicit parameters — never stored in `context.Context`.

```go
err := nucleus.WithTx(ctx, client, func(tx nucleus.Tx) error {
    _, err := tx.Exec(ctx, "INSERT INTO users (name) VALUES ($1)", "Alice")
    return err
})
```

## KV Client

Each method mirrors the Nucleus KV SQL function exactly:

```go
kv := kv.New(client)

kv.Set(ctx, "session:abc", data, kv.TTL(3600))
val, err := kv.Get(ctx, "session:abc")
kv.Del(ctx, "session:abc")
n, err := kv.Incr(ctx, "counter:views")
```

## Vector Client

```go
vec := vector.New(client)

vec.Insert(ctx, "embeddings", id, embedding)
results, err := vec.Search(ctx, "embeddings", queryVec, vector.K(10))
```

## Timeseries Client

```go
ts := ts.New(client)

ts.Insert(ctx, "events", value, ts.Tags{"host": "web-1"})
count, err := ts.Count(ctx, "events", ts.Since("-1h"))
avg, err := ts.RangeAvg(ctx, "events", ts.Since("-24h"))
```

## HTTP Server Bindings

Neutron Rust's routing model in Go. Handler returns an error — centralises error formatting in one place:

```go
type Handler func(c *RequestCtx) error
type Middleware func(next Handler) Handler

router := neutron.NewRouter()
router.Use(middleware.Logger())
router.Use(middleware.Recover())

router.GET("/api/users", func(c *neutron.RequestCtx) error {
    users, err := userRepo.List(c.Context())
    if err != nil {
        return err  // → 500
    }
    return c.JSON(200, users)
})

router.POST("/api/users", createUser)
```

## Config as Plain Struct

No functional-options chains. A plain struct is assignable, comparable, and loggable:

```go
cfg := nucleus.ParseConfig("postgres://localhost:5432/mydb")
cfg.MinConns             = 4                    // always-warm connections
cfg.MaxConns             = 20                   // (cores × 4) for SSD workloads
cfg.HealthCheckPeriod    = 30 * time.Second     // aggressive enough to catch dead connections
cfg.MaxConnIdleTime      = 5 * time.Minute      // reclaim idle connections
cfg.MaxConnLifetime      = 1 * time.Hour        // recycle before TLS cert expiry
client, err := nucleus.New(ctx, cfg)
```

## Batch Operations

Use `SendBatch` when issuing 5+ queries per request — it collapses multiple round-trips into one:

```go
batch := &pgx.Batch{}
batch.Queue("SELECT value FROM kv WHERE key = $1", "session:abc")
batch.Queue("SELECT value FROM kv WHERE key = $1", "session:xyz")
batch.Queue("SELECT id, name FROM users WHERE id = $1", userID)

results := client.SendBatch(ctx, batch)
defer results.Close()

val1, _ := results.QueryRow().Scan(&v1)
val2, _ := results.QueryRow().Scan(&v2)
user, _ := pgx.CollectOneRow(results.Query(), pgx.RowToStructByName[User])
```

For bulk inserts (thousands of rows), use `COPY` — it's 10–50× faster than batched `INSERT`:

```go
_, err = client.CopyFrom(ctx,
    pgx.Identifier{"vectors"},
    []string{"id", "embedding", "metadata"},
    pgx.CopyFromRows(rows),
)
```

## Error Handling

Nucleus propagates PostgreSQL error codes. Use `errors.As` to detect them:

```go
import "github.com/jackc/pgconn"

var pgErr *pgconn.PgError
if errors.As(err, &pgErr) {
    switch pgErr.SQLState() {
    case "40001": // serialization_failure — safe to retry
        return retryWithBackoff(ctx, fn)
    case "23505": // unique_violation — caller decides
        return ErrAlreadyExists
    }
}
```

Nucleus also defines data-model-specific codes (vector dimension mismatch, KV key conflict) that follow the same pattern.

## Observability

Wrap the Querier at the client layer — one place, covers all data models:

```go
func (c *instrumentedClient) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
    ctx, span := tracer.Start(ctx, "nucleus.query",
        trace.WithAttributes(
            attribute.String("db.system", "nucleus"),
            attribute.String("db.statement", sql),
        ),
    )
    defer span.End()
    return c.inner.Query(ctx, sql, args...)
}
```

## What We Took From Each Library

| Library | What we adopted |
|---------|----------------|
| pgx v5 | Wire protocol driver, `CollectRows`, `NamedArgs`, modular sub-packages |
| sqlx | `db` struct tag convention (already the Go standard) |
| bun | SQL-first query builder style — SQL stays visible |
| go-redis | Command-mirroring API for KV client |
| chi | Radix tree routing, 100% `net/http` compatible middleware |
| Echo | `func(*Ctx) error` handler signature |

## What We Avoided

| Library | What we avoided |
|---------|----------------|
| GORM | Silent zero-value semantics, invisible callbacks, `Save()` ambiguity, N+1 by default |
| ent | Codegen complexity, hundreds of generated files, teaches ent not Go |
| Fiber | No HTTP/2/3, incompatible with `net/http` ecosystem, `*Ctx` reuse data races |
| mongo-go-driver | Driver primitive types leaking into application structs |

## File Structure

```
go/
├── nucleus/                    # Core SQL client
│   ├── client.go
│   ├── config.go
│   ├── querier.go              # Querier interface
│   ├── rows.go                 # CollectRows, CollectOneRow
│   ├── tx.go                   # WithTx, Begin
│   ├── schema.go               # Struct tag reflection, DDL generation
│   └── go.mod
├── kv/                         # KV client module
├── vector/                     # Vector client module
├── ts/                         # Timeseries client module
├── doc/                        # Document client module
├── graph/                      # Graph client module
├── fts/                        # Full-text search module
├── geo/                        # Geo module
├── pubsub/                     # Pub/Sub module
├── neutron/                    # HTTP server bindings
│   ├── router.go
│   ├── context.go
│   ├── middleware/
│   │   ├── logger.go
│   │   ├── recover.go
│   │   ├── auth.go
│   │   └── ratelimit.go
│   └── go.mod
└── go.work                     # Go workspace linking all modules
```

## Status

Planned — not yet implemented.
