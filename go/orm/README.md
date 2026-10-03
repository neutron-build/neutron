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
pointers, plus the finite exact `Decimal`, native `UUID` and explicit-document
`JSON` codecs described below. Arrays, domains and custom codecs still require
separate qualification. Floating-point fields do not promise exact numeric semantics.

`NewColumn[Model, Value](table, "GoField")` validates that `Value` matches the
mapped field type. Columns from a distinct table scope cannot be mixed.
`Eq`, `Ne`, `Gt`, `Gte`, `Lt`, `Lte`, `And` and `Or` compose bound predicates;
`Eq(nil)`/`Ne(nil)` on nullable columns emit `IS NULL`/`IS NOT NULL`. Ordered
NULL comparisons are rejected. Query values use PostgreSQL placeholders.

`Query[Model]` provides `Where`, `OrderBy`, `Limit` and `Offset` value builders.
`Select` scans complete models; `SelectColumn` returns a typed slice and
`SelectPair` returns typed `Pair` values. `SelectOne` verifies zero/one/multiple
matches and refuses a caller-supplied limit/offset that could hide cardinality.
`SelectOptional` retains that check, returning `Nullable[Model]{Valid:false}`
for no row. `CompileSelect` validates and returns placeholder SQL and detached
scalar arguments without a database connection.

`In`/`NotIn` snapshot and bind finite lists; empty lists evaluate FALSE/TRUE.
NULL list elements retain SQL three-valued membership semantics, unlike the
explicit `Eq(nil)` NULL test. `CompareAny`/`CompareAll` accept the fixed
`Comparison` constants and compile finite scalar lists to equivalent OR/AND
comparisons; they do not require array codecs. `Not` negates a predicate.
`Order.NullsFirst()`/`NullsLast()` set explicit NULL placement, including joins.

`Stream(ctx, executor, table, query, maxRows, visit)` visits full models with
one model of application buffering and a mandatory positive finite row budget.
The first excess row returns `ErrStreamBudget` before delivery. Returning false
from `visit` stops normally. Stop, failure, panic and cancellation close native
rows; an owned Scope then releases its operation lease. This is native pgx
result streaming rather than a server DECLARE cursor. Server execution and
buffering are not bounded by this API; application query planning and timeouts
remain necessary. Visitor errors do not independently poison a borrowed or owned
transaction; return the error from the transaction callback when rollback is
required. Native result decode failures retain the Scope poisoning policy.

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
live mode fails rather than silently skipping. Additional native suites cover
owned transactions, bounded composite associations, exact scalar codecs, typed
inner/left joins and explicit hook workflows. Model tracking, migration and
introspection ownership, broader codec families and full GORM parity remain
outside this package's implemented contract.

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
statement-leading CREATE/ALTER/DROP, CALL, DO, multiple statements, ambiguous ordinary-string backslashes and
pgx QueryRewriter/NamedArgs arguments are refused. Typed query-core SQL uses
positional bound parameters and is admitted. This guard prevents accidental
lifecycle bypass; trusted functions can still change session state. This is
keyword-based admission, not a complete SQL parser: SELECT INTO can create a
table and EXPLAIN ANALYZE can execute the admitted data query.

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

Bounded explicit associations
-----------------------------

`Join(parentColumn, childColumn)` requires identical Go key types at compile
time. `NewRelation(parentTable, childTable, joins...)` validates immutable table
identity, nonempty composite keys and duplicate fields. Only nonnullable string,
bool, int, int32, int64 and the native `UUID` codec keys are supported. UUID key
identity uses its exact16 bytes, not formatted text; its all-zero UUID remains
a legitimate key, distinct from SQL NULL. Nullable keys (including `*UUID`), floating-point, other custom codecs and
implicit key conversion semantics remain unsupported. `Inverse`
reverses ownership explicitly; this is not foreign-key inference or cascading.

`LoadMany(ctx, executor, relation, parents, childQuery, budget)` returns one
`Association` per input slot, preserving duplicate parent keys and each original
parent value. Missing children use an empty slice. Distinct composite keys are
deduplicated and loaded through qualified, bound OR-of-AND predicates. Each
input slot owns a separate result slice. Child order follows `OrderBy`; absent
an order it is unspecified. `LoadOne` refuses multiple matches for a key and
does not conceal cardinality with a limit. Both methods may filter children.

`LoadBudget{MaxParents, MaxRows, BatchSize}` is mandatory and positive.
MaxParents counts input slots; MaxRows bounds expanded child output slots,
including repeated attachments for duplicate parents; BatchSize bounds keys per
statement. Each child consumes its parent-key multiplicity before any final
output slice is allocated. Excess rows return
`ErrLoadBudget` with no partial result, rather than truncating. Internal LIMIT
fetches one row beyond the remaining total budget only to detect excess.
PostgreSQL's parameter limit is checked before each query. Caller LIMIT/OFFSET
is refused because global pagination does not implement per-parent pagination.

Multiple batches do not establish a consistent snapshot. Use an owned
RepeatableRead or Serializable Scope when that guarantee is required. Exact Go
identity is required on returned keys; database types or collations that equate
distinct Go keys are not implicitly supported. No hooks, association writes,
soft deletion, lazy loading or full GORM parity is claimed.

```sh
python3 orm/verify_compile.py
go test ./orm -run TestPostgresCompositeAssociationLoading -count=1 -v
```

The native association gate compares independently fetched PostgreSQL rows,
including tenants sharing IDs, composite keys, inverse ownership, an orphan,
missing and duplicate inputs, a hostile search path, bounded batching, child
filters and excess-row/cardinality rejection.

Native COMMIT outcome gates also cover a real loopback wire proxy dropping the
server's COMMIT acknowledgment while independently proving durable rows, plus
deferred-constraint and Serializable COMMIT rejection:

```sh
go test ./orm -run 'TestPostgres(LostCommitAcknowledgment|DeferredCommitRejection|SerializationCommitRejection|ChildContextCancellation)' -count=1 -v
```

Exact native scalar codecs
--------------------------

The mapped scalar set additionally admits the exact exported `Decimal`, `UUID`
and `JSON` types and nullable pointers to them. Named user types with the same
underlying representation are not implicitly certified. The caller's pgx
registry and protocol remain unchanged; native numeric/UUID scanner and valuer
interfaces and JSON bytes scanning drive these codecs.

`ParseDecimal(text)` parses finite decimal/scientific text without float64.
`Decimal.String()` emits exact base-ten text and the represented fractional scale.
The pinned pgx binary decoder normalizes zero numeric to exponent zero, so a
server value `0.00` reads as `0`; numeric value is exact, but original formatting
or declared scale is not a general round-trip guarantee.
Native numeric scanning snapshots the coefficient, and `NumericValue()` returns
a detached native coefficient. Zero Decimal is invalid; `ParseDecimal("0")` is
numeric zero; nil `*Decimal` is SQL NULL. NaN and both infinities are refused on
input and native reads. PostgreSQL17 unconstrained numeric's documented maximum
131072 integral digits and 16383 fractional digits bound formatting/allocation
([PostgreSQL numeric types](https://www.postgresql.org/docs/17/datatype-numeric.html)).
A column's declared numeric precision/scale can still round or reject values on
the server; the library does not infer or override those declarations.

`ParseUUID(text)` requires the standard hyphenated form and accepts upper/lower
hex; `UUID.String()` emits lowercase. UUID's zero value is the valid all-zero
UUID, while nil `*UUID` supplies SQL NULL. Parsing does not invent an ID or
silently accept malformed separators.

`ParseJSON(text)` validates an immutable document without decoding numbers into
float64 or collapsing duplicate object keys. `JSON.String()` returns stored
text. `ParseJSON("null")` is JSON null (`IsNull()` true); nil `*JSON` is SQL NULL.
Zero JSON is invalid and refused. This distinction survives model reads,
`SelectColumn` and `SelectPair` through a dedicated nullable bytes destination;
generic `**T` JSON unmarshalling can conflate these cases and is not used.
JSON values support both json and jsonb native columns; PostgreSQL jsonb may
normalize whitespace/key ordering or collapse duplicate keys, while json retains
text. Server validation/rejection remains authoritative. MarshalJSON emits a
Decimal as a quoted exact string, UUID as a quoted UUID and JSON as its document.

```sh
go test ./orm -run 'Test(Decimal|UUID|JSON|Scalar)' -count=1
go test ./orm -run TestPostgresExactScalarCodecs -count=1 -v
```

The native gate uses independent SQL text casts and microsecond epoch reads to
check int8 extrema, exact long numeric/scientific writes, UUIDs, microsecond
instants with offsets, json/jsonb, nullable numeric/UUID, SQL NULL versus JSON
null, projection decoding, default/NULL writes and explicit non-finite refusal.

UUID association qualification extends the native composite-key gate:

```sh
go test ./orm -run 'Test(AssociationUUIDExactIdentityAndNullPolicy|PostgresUUIDCompositeAssociations)' -count=1 -v
```

It compares independently scanned native UUID bytes/rows under a hostile search
path, including tenants sharing UUIDs, distinct UUIDs, zero UUID, duplicate
input fanout budgets, inverse ownership, missing/orphan rows and nullable-key
refusal. A mapped nullable UUID foreign key requires a future explicit nullable
relation policy; this slice does not equate NULL values or infer that policy.

Owned Scope result decoding failure
-----------------------------------

Actual `Scope` rows `Scan` or `Values` failures mark that Scope failed with
`ErrScopeDecode`, retaining the original cause. Further operations on that
Scope are refused. An outer callback swallowing the decode error still rolls
back and reports `CommitNotAttempted`; it cannot silently commit earlier writes.
A child savepoint swallowing its decoder error is rolled back to and released;
the child error is returned, and the parent may resume after successful cleanup.
Cleanup failure retains the separate transaction-broken/discard policy.
Validation errors detected before query execution do not poison a Scope.
Borrowed native Executors retain their caller-owned transaction policy.

```sh
go test ./orm -run 'Test(ScopeDecodeFailureCannotCommitWhenSwallowed|ChildDecodeFailureRecoveredOnlyBySavepointRollback|PostgresScopeDecodeFailureRollbackAndChildRecovery)' -count=1 -v
```

Typed two-table SQL joins
-------------------------

`NewInnerJoin(relation)` and `NewLeftJoin(relation)` validate an exact-key
relation between two distinct qualified physical tables. They support composite
keys across schemas, including tables with the same basename in different
schemas. Repeating the same physical schema/table is refused even through a
separately created table handle; explicit self aliases remain unsupported.
This API is additive: association loading and existing single-table queries
retain their APIs.

`JoinParentField(scope, column)` projects the parent's original scalar type.
`InnerChildField(innerScope, column)` projects the child's original type.
`LeftChildField(leftScope, column)` necessarily projects `Nullable[T]`, including
when the original mapped T is already a pointer. Its `Valid=false` means SQL
NULL and its `Value` is then zero. That is not matched-row-presence detection:
unmatched rows and matched nullable fields can both produce SQL NULL. An actual
non-NULL zero/false remains Valid=true. JSON null remains a valid JSON document;
SQL NULL remains invalid in the outer wrapper.

`scope.Query()` builds a `JoinQuery[P,C]`. `WhereParent` and `WhereChild` accept
predicates of the correct model and combine them with AND in call order.
`WhereChild` is a WHERE filter, not an ON filter: a condition excluding NULL
removes unmatched LEFT JOIN rows. `OrderParent`/`OrderChild` accept their model's
orders, preserve call order and use PostgreSQL's default NULL placement.
`Limit` and `Offset` bind nonnegative joined-row counts, not parent pagination.
Fields/predicates/orders retain exact table and join binding checks even where
different tables share a Go model or physical names. Equal metadata recreated
for another binding does not silently enter the query.

`SelectJoinedColumn` and `SelectJoinedPair` return typed scalar/pair slices.
`SelectJoinedOne` and `SelectJoinedPairOne` require exactly one joined result;
they refuse any explicit LIMIT/OFFSET, including zero, before database effects
and internally fetch at most two rows. They preserve `ErrNotFound`,
`ErrCardinality`, context and native SQLSTATE causes. Execution uses the caller's
Executor and the existing Scope lease. Actual Scope decoder errors poison that
Scope as documented above; borrowed Executors retain caller lifecycle policy.

The bounded slice excludes arbitrary ON expressions, OR across parent/child
predicates, multiple joins, self aliases, aggregates, expression projections,
joined full-model identity materialization and implicit per-parent pagination.
Nullable[T] is a projection wrapper rather than a writable mapped codec.

```sh
python3 orm/verify_compile.py
go test ./orm -run 'Test(Joined|PostgresTypedInnerLeftJoinProjections)' -count=1 -v
```

Compile consumers verify model ownership, required nullable left-child results
and correct scalar/pair result types. Native joins compare handwritten SQL row
oracles for qualified schemas/composite keys, left NULL versus real zero/false,
JSON null, ordering/bound filtering/pagination, cardinality, Scope lease cleanup,
SQLSTATE preservation and swallowed decoder rollback. Required-live mode and a
disposable PostgreSQL URL remain necessary for native qualification.


## Explicit owned hook workflows

Construct `NewHookRepository(table, HookSet[Model]{...})` with immutable copied
registration slices. `WithHookTransaction(ctx, pool, options, callback)` provides
an owned `*WriteSession`; call `HookInsert`, `HookUpdate` or `HookDelete` explicitly.
Borrowed executors and raw SQL never invoke hooks automatically.

Each statement invokes BeforeWrite, its BeforeCreate/BeforeUpdate/BeforeDelete
hooks, the validated core write, its AfterCreate/AfterUpdate/AfterDelete hooks,
then AfterWrite. Registration order is preserved. Updates and deletes invoke
statement hooks once, reporting the actual affected count including zero;
there is no implicit per-row model loading. Insert events contain the INSERT
RETURNING snapshot, which may differ from the final row after extra hook SQL.

Before hooks receive a read-only `WriteIntent`. `InspectHookValue(intent, column)`
returns a detached typed value and omission/supplied/default mode; it never
replaces assignments or predicates. Each after/commit hook receives a detached
model snapshot. Hooks can execute explicit extra SQL through their callback's
`*HookContext` Executor. It enforces the owning Scope's native leases and
cancellation; background operation contexts cannot bypass callback cancellation.
The capability becomes terminal when its callback returns. Callback contexts
and the session expose no raw native connection or transaction controls.

Initial SQL/metadata validation happens before hooks and may be recovered by the
application. Any error, panic, cancellation, ignored reentry or result decode
failure after a workflow starts requires rollback even if the application
swallows it. Captured-session reentry returns ErrHookReentry; overlapping session
operations return ErrHookBusy. No mutex is held across user callbacks and no
Go goroutine identity is inferred. Callbacks must join their goroutines and
close results before returning; active rows/operations are treated as leaks.
An arbitrary callback that ignores its context cannot be forcibly terminated.

`session.Savepoint(ctx, callback)` creates an owned child write session.
Successful child rollback drops its notification queue and allows the parent to
resume. Events merge into the parent's statement order only after native RELEASE
succeeds. Cleanup failure retains the Scope's whole-transaction discard policy.

After PostgreSQL acknowledges COMMIT, the connection is released and the session
is terminal before queued AfterCommit hooks run synchronously in statement and
registration order. DispatchTimeout defaults to five seconds and cooperatively
bounds dispatch; the first error stops subsequent callbacks. Errors return
`CommittedDispatchError`; panics propagate as `CommittedHookPanic`. Both explicitly
mean the database committed and side effects may be partial. Rejection, rollback
and indeterminate COMMIT never dispatch. Nothing retries writes or notifications;
crashes and lost acknowledgements require application reconciliation or a durable
outbox. This is not exactly-once or durable notification delivery. Hook closures
and external shared state remain application-owned synchronization responsibilities.


## Explicit application scopes, soft deletion and version guards

`NewScopedTable(table, predicate)` validates a fixed immutable application scope.
Its `Query`, `Select`, `SelectOne`, `Update` and `Delete` retain that scope. Writes
additionally require an explicit valid caller predicate; a tenant scope cannot
hide a missing write condition. Core operations and raw SQL remain explicit.

`NewSoftDelete(scoped, nullableTimeColumn)` opts into a nullable timestamp deletion
marker. `Active` and `Select` include the application scope and IS NULL marker.
`IncludingDeleted` includes the same application scope and permits deleted rows.
`Remove` supplies an explicit timestamp; `Restore` sets NULL on deleted matches;
`Update` only modifies active matches and refuses direct deletion-marker changes.
Policies and query builders are values: administrator reads or restoration never
mutate another caller's active policy. Relation loaders can accept `Active(query)`
explicitly; hooks and core Delete are not automatically intercepted.

`VersionedUpdate(ctx, scope, table, predicate, int64VersionColumn, expected,
assignments...)` requires an owned Scope, an explicit predicate and nonempty
zero-safe assignments. One statement matches the expected nonnegative int64
version and increments it. Callers cannot assign the version column themselves.
A missing/stale match returns `ErrVersionConflict`; multiple matches return
`ErrCardinality`. Both poison the Scope with `ErrScopeMutation`, ensuring that
swallowing the guard error cannot commit prior writes or a multi-row change.
Successful child savepoint rollback permits parent recovery. The caller must
include its tenant/identity scope in the predicate; no primary key is inferred.

```sh
go test ./orm -run 'Test(ImmutableScopedSoftDeletePolicy|VersionGuardPoisonAndValidation|PostgresScopedSoftDeleteAndVersionGuards)' -count=1 -v
```
