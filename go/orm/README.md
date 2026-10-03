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
separately created table handle; use NewAliasedInnerJoin or NewAliasedLeftJoin for explicit aliases, including self relations.
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
predicates, multiple joins, aggregates, expression projections,
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


## Explicit bounded relation graph writes

`CreateRelated`, `AppendRelated`, `SaveRelated`, `ReplaceRelated` and
`DeleteRelated` accept the exact `Relation` binding and its parent/child
`HookRepository` registrations inside a `WithHookTransaction` callback. Each
operation owns a child savepoint. Failure rolls back the entire operation even
if its error is swallowed; only acknowledged RELEASE merges post-commit events.
The parent session stays reserved for the graph lifetime, including between
statements. Additional unrelated parent work may continue after clean rollback.

Create prevalidates every child assignment plan before any parent hook or write.
Child foreign-key columns are derived from the actual returned or locked parent
composite key; caller overrides are refused. Existing-parent operations lock and
check exactly one matching composite identity. Save updates explicit non-key
parent assignments, including empty/zero values, then appends newly inserted
children. It does not infer tracked changes or child upserts. Replace explicitly
deletes old children then inserts replacements; Delete explicitly deletes
children then the parent. These operations perform orphan deletion rather than
nullable disconnection or inferred database cascades. Parent keys are immutable
in Save. Self/cyclic physical-table edges refuse before effects.

`GraphBudget` bounds proposed and removed child rows and planned core SQL
statements. Counts include parent locks, child budget reads and writes;
savepoint/transaction control and hook extra SQL are separate overhead. A bounded
child read precedes deletion, and actual DELETE count is checked again to catch
external unconstrained writers. The schema must enforce the intended unique
parent and composite FK constraints: relation metadata does not install or
certify them. Arbitrary deep graphs, deferred cyclic key plans, nullable
disconnection and implicit many-to-many mutation remain unimplemented.

```sh
go test ./orm -run 'Test(GraphPrevalidationAndCompositeKeyDerivation|PostgresOwnedRelationGraphMutations)' -count=1 -v
```


## Bounded native batch, upsert and COPY

`InsertBatch(ctx, scope, table, assignmentRows, maxRows)` validates all typed
assignment rows before effects and executes bounded INSERT RETURNING statements
in one child savepoint. A final-row failure rolls back earlier rows, even if
the caller swallows the returned error. It is a bounded batch, not one SQL
statement, and does not implicitly run repository hooks.

`BindColumn(column)` retains model/table identity while erasing only its value
type for explicit COPY column and conflict-key lists. `UpsertOne` targets an
explicit server-enforced unique key. Nonempty update columns are assigned from
EXCLUDED; key changes and updates of omitted insert columns are refused. Supplied
zero/NULL values remain supplied. Empty updates mean DO NOTHING and return
`Nullable[Model]{Valid:false}` on conflict. Successful insert/update returns its
statement snapshot without claiming which branch occurred. The owned child
savepoint protects failure/cardinality boundaries; no mutation is retried.

`CopyInto(ctx, scope, table, boundColumns, maxRows, pgxSource)` uses native pgx
COPY FROM on the pinned transaction with one row of validation buffering. Values
must exactly match bound column Go scalar types; nullable NULL and dynamic shape
are validated. Budget overflow, source/codec/native failures roll back every
row in the operation-owned child savepoint and return count zero. Sources must
cooperate with cancellation in their Next/Values methods. This API requires
native COPY support and returns `ErrCopyUnsupported` for a driver lacking it.
It does not certify Nucleus COPY or automatically invoke hooks.

```sh
go test ./orm -run 'Test(BulkAdmissionAndCopySourceBudgets|PostgresOwnedBulkUpsertAndCopy)' -count=1 -v
```


`NewAliasedInnerJoin` and `NewAliasedLeftJoin` validate two distinct quoted aliases
and retain explicit parent/child roles even when both use the same Table metadata.
Parent/child projection, filter and ordering methods always refer to their role;
left-child fields still require Nullable results. Reusing a projected field from
another aliased binding is refused. The original join constructors continue to
refuse duplicate physical tables without explicit aliases.

```sh
go test ./orm -run 'Test(AliasedSelfJoinRoleBindings|PostgresAliasedSelfJoinRoleProjections)' -count=1 -v
```


Application predicate scopes are query policy; PostgreSQL row-level security is
a separate authorization boundary. The native gate below provisions an actual
NOSUPERUSER/NOBYPASSRLS login and a size-one pool, binds an authenticated tenant
through transaction-local `set_config`, and compares ORM reads with an independent
native connection using that role. It covers alternating tenants, no-context
reads, child rollback/release, idle callback cancellation, native query failure
and denied tenant-changing writes, checking reuse of the same physical pooled
connection. The application is trusted to select authenticated tenant context;
this gate does not certify arbitrary tenant-spoofing SQL or external poolers.
The fixture owns a disposable random role and schema; its generated password
is never logged. The test database must permit fixture role provisioning.

```sh
go test ./orm -run TestPostgresLeastPrivilegeTenantRLSAndPoolReset -count=1 -v
```


`SeekAfter(table, query, uniqueColumns, cursorAssignments)` adds a bound
lexicographic predicate for the complete query order. It handles ties, mixed
ascending/descending order and explicit/default NULL placement without OFFSET.
The cursor must supply every ordered column exactly in order; the declared
nonnullable unique key must appear in that order. The database schema must
enforce the declared uniqueness; metadata does not certify it. Values and query
builders remain immutable. Ordinary READ COMMITTED keyset pages are not a
consistent snapshot under concurrent insert/update/delete; use an appropriate
owned transaction when one snapshot is required.

```sh
go test ./orm -run 'Test(KeysetNullLexicographicAndUniqueAdmission|PostgresKeysetTiesAndNullOrdering)' -count=1 -v
```


## Typed native aggregates

`CountAll`, `CountColumn`, `CountDistinct`, `Min`, `Max`, `SumInt64`, `AvgInt64`,
`SumInt32`, `AvgInt32`, `SumDecimal`, `AvgDecimal`, `SumFloat64` and `AvgFloat64`
retain exact table/model and result types. PostgreSQL SUM/AVG(bigint) return exact
Decimal results, not int64 or floating point. Empty nullable aggregates return
Nullable.Valid=false; COUNT returns zero. `AggregateOne` refuses ungrouped
ordering/pagination that could conceal its single-row contract. `SelectGrouped`
returns typed group-key/aggregate pairs; Compare supplies a bound HAVING condition
and only the group key can order results. Group/HAVING handles from another table
binding are refused. Aggregate codec families unsupported by PostgreSQL retain
native errors; this bounded API does not claim arbitrary multi-key grouping or
universal expression algebra.

```sh
go test ./orm -run 'Test(TypedAggregateCompilationAndOwnership|PostgresExactTypedAggregatesAndHaving)' -count=1 -v
```


`Over(aggregate, partitionColumns, orders...)` preserves an aggregate's exact
result/nullability for window queries. `RowNumber` yields int64. `RowsBetween`
accepts explicit unbounded/current/preceding/following boundaries and binds finite
nonnegative row offsets. `SelectWindowPair` returns an original typed scalar and
typed window result; input WHERE precedes the window, output ordering/pagination
follows it. PostgreSQL's default frame remains unchanged unless explicit.
Distinct aggregate windows refuse before execution because PostgreSQL does not
support them. Native errors retain other invalid frame/type cases.

```sh
go test ./orm -run 'Test(WindowFrameBindingsAndOwnership|PostgresTypedWindowsAndFrames)' -count=1 -v
```


`NewLateralInnerJoin`/`NewLateralLeftJoin` correlate exact composite relation keys
to each parent, with explicit quoted parent/child aliases. Their child Query
requires a finite explicit per-parent LIMIT and independently applies bound
filter, order and OFFSET. These are native LATERAL queries: every parent receives
its own child paging policy. Left joins retain missing parents and require
Nullable child projections; outer WHERE filters still follow ordinary SQL
semantics. Filters/projections/order retain the sealed join/table binding,
including same-metadata self relations. Arbitrary raw correlated SQL is not
accepted by these constructors.

```sh
go test ./orm -run 'Test(LateralPerParentCompilationAndBinding|PostgresCompositeLateralPerParentPaging)' -count=1 -v
```


## Typed CTE and set-operation handles

`NewModelQuery(table, query)` represents a complete-model SELECT. `Union`,
`UnionAll`, `Intersect` and `Except` require the same model type and compatible
full projection metadata. Plans retain independently bound branch values and
branch pagination. `SelectModels` returns complete models.

`NewCTE(alias, plan)` returns a sealed derived binding. `DerivedColumn` creates
typed columns for that binding; original table columns cannot silently filter
its scope. `SelectDerived` and `SelectDerivedColumn` query the CTE with explicit
typed filtering, ordering and pagination. `Query.Distinct` emits native DISTINCT
for the actual projection; unsupported native equality codecs retain SQL errors.

`NewRecursiveCTE(alias, anchor, relation, maxDepth, maxRows)` traverses exact
relation edges from a complete-model anchor. Depth is explicit from 0 through
128; maxRows bounds delivered results and rejects overflow without exposing
partial models. UNION ALL preserves multiplicity, including cycles, which stop
at the depth bound. It does not claim acyclic input or bound PostgreSQL internal
execution/memory: appropriate statement deadlines remain necessary, especially
for branching graphs or output sorting. No arbitrary raw recursive SQL is
interpolated. Arbitrary correlated subquery expression projections are not
accepted by these constructors.

```sh
go test ./orm -run 'Test(TypedSetAndDerivedBindingCompilation|PostgresTypedCTESetAndBoundedRecursiveQueries)' -count=1 -v
```

## Distributed typed column generator

The SDK module includes `cmd/neutron-ormgen`; install it from the same pinned
module version as the runtime. Generation reads an explicit struct's `db` tags
without connecting to a database. It produces a typed column bundle and a
constructor that checks runtime field types and tags before creating a table.
`-check` compares the deterministic generated source without writing it. The
fingerprint describes Go mapped metadata; it does not certify live database DDL.

```sh
go run github.com/neutron-build/neutron/go/cmd/neutron-ormgen -dir . -type Record -out record_columns.gen.go
go run github.com/neutron-build/neutron/go/cmd/neutron-ormgen -dir . -type Record -out record_columns.gen.go -check
```

Generated `NewRecordTable(schema, name)` returns the table and `RecordColumns`;
`columns.Name.Eq(1)` and nonexistent column members fail compilation. Generic,
embedded, untagged, conflicting and unqualified codec fields refuse generation.
Ignored `db:"-"` fields have no column. Reflection construction caches immutable
validated Go shapes while preserving independent table identity for every
constructor, including concurrent calls.

```sh
go test ./cmd/neutron-ormgen ./orm -run 'Test(Generator|GeneratedOutside|MetadataCache)' -count=1 -v
```

## Dimension-preserving arrays and binary values

`NewBytea` owns binary bytes, including embedded NUL; empty and SQL NULL remain
distinct. `NewArray[T]` owns flat scalar elements and explicit native PostgreSQL
dimensions/lower bounds. Nullable element types such as `Array[*int64]` and
`Array[*JSON]` preserve SQL NULL slots; JSON null remains a valid JSON document.
`*Array[T]` represents a nullable column. Valid empty arrays have zero dimensions;
zero codec values refuse writes. Constructors and accessors detach pointers and
bytes from caller storage. Native pgx array scanning preserves up to six
dimensions with a mandatory 1,000,000 element allocation bound. Flat Go slices,
nested Array element types and uncertified element codecs refuse mapping.
The distributed generator also emits Array and Bytea typed columns.

```sh
go test ./orm -run 'Test(ImmutableArray|Bytea|PostgresDimensionPreserving)' -count=1 -v
```

`Range[T]` preserves native inclusive/exclusive/unbounded/empty bounds for int32,
int64, Decimal, time.Time and Date. PostgreSQL canonicalizes discrete ranges;
reads and RETURNING reflect the native canonical value. SQL NULL uses a nullable
pointer and remains distinct from empty and unbounded ranges. Zero Range values
refuse writes. `Date` preserves finite Gregorian calendar parts in years 1–9999;
date infinity and values outside that profile refuse. `TimeOfDay` retains integer
microseconds including the distinct 24:00:00 endpoint; `Interval` keeps months,
days and microseconds independently. timetz and multiranges remain unqualified.
time.Time writes require whole microseconds, refusing silent submicrosecond
truncation. Timestamp without zone follows native pgx wall-clock semantics;
timestamptz retains the instant, not the original named zone/offset.

```sh
go test ./orm -run 'Test(RangeNative|FiniteTemporal|PostgresRangesAndFinite)' -count=1 -v
```

`NewPostgresTable` adds native qualified catalog admission before returning
write-capable metadata: mapped types and nullability must match the accepted OID
matrix. Domains retain their native identity and constraints while their base
codec is qualified; `Enum` preserves exact labels with native membership checks.
Unknown composites/custom base OIDs, enum-as-string, numeric-as-float and wrong
range subtypes refuse with schema/table/column/type/OID identity before any
mutation. No DDL, registry changes or search_path assumptions occur. This is
point-in-time metadata for that database: rebuild it after DDL. Database-free
`NewTable` validates Go shape and does not certify PostgreSQL catalog types.
Preserving refusal leaves unsupported objects and values intact; it does not
claim typed composite, network, pgvector or multirange support.

```sh
go test ./orm -run 'Test(CatalogCodecMatrix|PostgresCatalogEnumDomain)' -count=1 -v
```

`SQLValue[T]` adapts explicit database/sql Scanner+Valuer implementations. It
freezes native driver values, detaches binary buffers, and reconstructs `T` only
on explicit `Decode`; mutable custom objects cannot change earlier assignments.
Nonnullable adapters reject Valuer SQL NULL and zero values. `CodecFor[M,T]`
pins a mapped field to exact native schema/name/OID; pass that contract to
`NewPostgresTable` to admit a custom type. Missing/wrong contracts refuse before
mutation. Codec authors own semantic fidelity; supplying a contract does not
certify an arbitrary parser. The tested opaque composite adapter preserves
native text and int64 precision without claiming structured composite mapping.
Native decode failures retain owned Scope rollback guarantees.

```sh
go test ./orm -run 'Test(CustomScanner|PostgresExplicitCustom)' -count=1 -v
```

`FromCTE` creates a complete-model plan over a derived binding and carries its
definitions through later CTEs and set operations. Definitions are emitted once
in dependency order with continuous parameter numbering. `WithCTEs` includes
additional sealed definitions, including different mapped models; conflicting
aliases, zero handles, cycles and more than 64 definitions refuse. Recursive
CTEs retain `SelectDerived` result-budget ownership and cannot be converted into
an unbounded FromCTE plan. No alias or source SQL is accepted as raw text.

```sh
go test ./orm -run 'Test(MultipleCTE|PostgresMultipleCTE|TypedSet|PostgresTypedCTE)' -count=1 -v
```

`NewScalarQuery` and `ScalarFromCTE` retain a one-column Go type. `InSubquery`,
`NotInSubquery` and `CompareSubquery` require identical outer/inner value types,
continuous bound parameters and native SQL NULL/cardinality semantics. `Exists`
accepts a complete-model plan. `ExistsRelated` correlates exact composite/self
relation keys with typed child filters and paging; nested self subqueries retain
separate aliases. `SelectRelatedScalar` returns a parent value plus Nullable child
scalar. Missing children and present SQL NULL share the SQL NULL result. Multiple
unpaged children retain native SQLSTATE 21000; explicit LIMIT means explicit
caller paging. Errors return no partial projections and keep Scope rollback.

```sh
go test ./orm -run 'Test(TypedSubquery|PostgresTypedScalar)' -count=1 -v
```

`NewBoundSQL` provides a trusted developer SQL escape hatch with immutable
qualified scalar parameters. Values remain separate pgx arguments; SQL text is
not constructed from them. `SelectBound` requires a positive result budget and
validates every native result column's name, position and OID before returning
complete models. Omitted, reordered, renamed and incompatible fields refuse;
native catalog-qualified custom OIDs must retain their recorded identity.
Overflow/errors return no partial models. Projection/budget rejection poisons an
owned Scope even when swallowed. The SQL itself is trusted application code,
may contain native writes/CTEs, and makes no read-only assertion. Borrowed native
connections retain caller transaction policy and protocol choice.

```sh
go test ./orm -run 'Test(BoundSQL|PostgresBoundSQL)' -count=1 -v
```

`NewThroughRelation` joins explicit parent-to-link and link-to-target relations
with the same intermediate binding. `LoadThrough` preserves composite tenant
identity, parent input positions, link order and duplicate multiplicity. Each
link must target at most one child; missing/filtered targets follow inner-join
semantics. Separate parent/link/output/batch budgets count expanded duplicates;
batched eager loading avoids one query per parent. Global paging refuses, and
cross-batch consistency remains caller-owned snapshot isolation. Target deletion
and join-table cascades are never inferred from this read mapping.

```sh
go test ./orm -run 'Test(ThroughRelation|PostgresCompositeManyToMany)' -count=1 -v
```

`NullableRelation` explicitly separates required shared identity fields
(`JoinRequired`, e.g. tenant) from nullable FK components (`JoinNullable`).
`LoadNullableMany/One` preserve exact nullable scalar key decoding. Owned
`ConnectNullable`, `ReparentNullable`, `DisconnectNullable`, `UpdateNullable` and
`DeleteNullableChild` lock exactly one parent/child in an operation savepoint.
Connect permits unowned/same-owner children; changing owner requires explicit
Reparent, which still cannot change required shared identity. Partially NULL
composite keys refuse; disconnect clears nullable FK fields and retains the row.
Update cannot override FK fields; delete is explicit orphan deletion. Each
workflow enforces one-child/three-core-statement budgets, count-one mutations,
native FK constraints and hook event rollback on operation failure. Nullable
self cycles and inferred cascading remain separate unsupported graph plans.

```sh
go test ./orm -run 'Test(NullableRelation|PostgresNullableRelation)' -count=1 -v
```
