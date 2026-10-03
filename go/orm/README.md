# PostgreSQL Go typed query core

Import `github.com/neutron-build/neutron/go/orm` independently of the Nucleus
client. Pass a caller-owned `pgx.Conn`, `pgxpool.Pool`, or `pgx.Tx` as `Executor`.
The query core preserves the executor's protocol settings and never owns the
borrowed executor's lifecycle. The opt-in `WithTransaction` helper described
below owns its pinned connection and transaction. Neither API retries writes
or changes `go/nucleus` behavior.

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
rich codec coverage, or GORM parity.

## Owned transactions and savepoints

`WithTransaction(ctx, pool, options, callback)` accepts a native `pgxpool.Pool`,
pins one connection, and passes a `*Scope` Executor to the callback. It commits
only after the callback returns successfully, every result closes or drains,
and the transaction context remains live. Errors, panics, and leaked operations
roll back with a separate bounded cleanup context. Failed cleanup discards the
connection. The original panic is re-raised unless cleanup also failed, in
which case `PanicCleanupError` carries the original value and cleanup error.

`TransactionOptions` accepts ReadCommitted, RepeatableRead, Serializable or
the server default; ReadOnly, ReadWrite or the server default; and Deferrable
only with explicit Serializable/ReadOnly. CleanupTimeout defaults to five
seconds. These are initial PostgreSQL settings, not a SQL security sandbox.

`scope.Savepoint(ctx, callback)` gives a child Scope ownership of a generated
savepoint. Parent operations are suspended until the child finishes. Child
error/panic rolls back to and releases its savepoint before parent work resumes.
Failed child cleanup marks the entire transaction broken, and the outer owner
rolls back and discards the connection even if the callback swallows that error.

Each Scope operation holds one lease until Exec finishes or Query rows close
or drain. Overlapping operations return `ErrConcurrentUse`; parent interleaving
returns `ErrParentSuspended`, and terminal handles return `ErrScopeClosed`.
Sequential use from different goroutines is permitted: the implementation does
not inspect goroutine identity. Callbacks must join their goroutines before
returning and close results. Scope rows do not expose the raw connection through
`Rows.Conn`. Concurrent row methods are rejected; caller-owned custom Scan
destinations must not block indefinitely. Cleanup can detach a busy connection
and close its concurrency-safe socket, then wait for native driver use to end;
it cannot terminate arbitrary application goroutines or blocking custom codecs.

Raw Scope Query/Exec accept a single SELECT, INSERT, UPDATE, DELETE, WITH,
VALUES, or EXPLAIN statement. Direct transaction controls, savepoint commands,
DDL, CALL, DO, multiple statements, ambiguous ordinary-string backslashes and
pgx QueryRewriter/NamedArgs arguments are refused. Typed query-core SQL uses
positional bound parameters and is admitted. This guard prevents accidental
lifecycle bypass; trusted functions can still change session state.

`TransactionError.Outcome` distinguishes `CommitNotAttempted`, `CommitRejected`,
and `CommitUnknown`. Cancellation before calling COMMIT is not attempted;
transport/cancellation failures after invoking COMMIT are conservative unknowns
unless the driver proves no bytes were sent. Explicit definite rejection states
and pgx's ROLLBACK response are rejected. SQLSTATE class 08 and 40003 remain
unknown. A nonempty SQLSTATE alone never proves rollback. Failed COMMIT always
discards its connection and never retries. `ErrCommitAmbiguous` identifies
unknown outcomes; native causes remain available through errors.Is/errors.As.

Run the owned transaction native cases with the same disposable URL and required
live mode:

```sh
go test ./orm -run 'TestPostgres(OwnedTransactions|ScopeRowsLeakAndConcurrency|TransactionCancellationAndServerTimeout|ReadOnlyDeferrableAndFailedOuterRecovery)' -count=1 -v
```

Fixture lifecycle tests separately exercise simulated lost COMMIT responses,
SQLSTATE 08007/40003, cleanup failure and no-replay/discard decisions. Native
tests establish actual savepoint rollback, pool reuse, cancellation and failed
transaction recovery; fixture tests do not substitute for those database gates.
