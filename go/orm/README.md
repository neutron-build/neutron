# PostgreSQL Go typed query core

Import `github.com/neutron-build/neutron/go/orm` independently of the Nucleus
client. Pass a caller-owned `pgx.Conn`, `pgxpool.Pool`, or `pgx.Tx` as `Executor`.
The package preserves the executor's protocol settings and never opens/closes a
pool, commits a transaction, retries a write, or changes `go/nucleus` behavior.

`NewTable[Model](schema, table)` validates mapped struct fields and explicit
`db` tags. `db:"-"` excludes a field; nullable fields use pointers with
`db:"column,nullable"`. Qualified identifiers are quoted, including embedded
quotes; NUL/empty/overlong identifiers are refused. Current scalar support is
string, bool, int/int32/int64, float32/float64 and time.Time, with nullable
pointers. Exact decimal, JSON, arrays, domains and custom codecs are not yet
implemented. Floating-point fields do not promise exact numeric semantics.

`NewColumn[Model, Value](table, "GoField")` validates that `Value` matches the
mapped field type. Columns from a distinct table scope cannot be mixed.
`Eq`, `Ne`, `Gt`, `Gte`, `Lt`, `Lte`, `And` and `Or` compose bound predicates;
`Eq(nil)`/`Ne(nil)` on nullable columns emit `IS NULL`/`IS NOT NULL`. Ordered
NULL comparisons are rejected. Query values use PostgreSQL placeholders.

`Query[Model]` provides `Where`, `OrderBy`, `Limit` and `Offset` value builders.
`Select` scans complete models; `SelectColumn` returns a typed slice and
`SelectPair` returns typed `Pair` values. `SelectOne` verifies zero/one/multiple
matches and refuses a caller-supplied limit/offset that could hide cardinality.

`Set(column, Some(value))` supplies a write value, including false/zero/empty
string. `Some((*string)(nil))` supplies SQL NULL to a nullable string.
`Default[T]()` emits SQL DEFAULT; `Omit[T]()` and a zero `Optional[T]` omit the
assignment. Nullable scalar pointers are snapshotted when an assignment or
predicate is constructed. Duplicate assignments are refused. An entirely
omitted insert emits DEFAULT VALUES; an entirely omitted update is refused.

`InsertOne` executes one VALUES tuple with RETURNING and checks result
cardinality. `Update` and `Delete` require explicit nonempty predicates and
return affected-row counts; they do not promise exactly-one mutation or
automatic rollback. Put operations in a caller-owned transaction when your
application requires that boundary. Decode/network errors after writes do not
establish rollback and must not trigger blind replay.

All execution takes `context.Context`. Errors unwrap their native causes,
including `pgconn.PgError` and cancellation. `Error.SQLState()` exposes the
PostgreSQL code; its display text excludes server messages that may contain
parameter values. The retained native cause can contain sensitive details.

Run `python3 orm/verify_compile.py` from `go/` for pure contracts and public
positive/negative compile consumers. For actual PostgreSQL verification, set
`NEUTRON_ORM_TEST_DATABASE_URL` to an owned disposable PostgreSQL database and
`NEUTRON_ORM_REQUIRE_LIVE=1`, then run:

```sh
go test ./orm -run TestPostgresCore -count=1 -v
```

The live test owns temporary schemas, uses a hostile shadow search_path,
checks native pgx state and SQLSTATE, exercises typed projections and scoped
CRUD, and cleans up its schemas. Without the URL it skips by default; required
live mode fails rather than silently skipping. This first core does not include
associations, hooks, model tracking, migration/introspection ownership,
transaction management, savepoint helpers, rich codec coverage, or GORM parity.
