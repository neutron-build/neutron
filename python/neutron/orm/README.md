# Opt-in PostgreSQL SQL core

Install `neutron-framework[orm]`. Import from `neutron.orm`. Existing
`neutron.nucleus` asyncpg clients are unchanged.

This is a bounded native psycopg synchronous/asynchronous SQL core with
explicit scalar dataclass Sessions and dependency-ordered insert graphs. Cascades and
full SQLAlchemy parity remain outside this slice. Only `postgres-direct` is admitted.
`Database.connect` and `AsyncDatabase.connect` compare the startup server
version with `pg_catalog.version()` before handing out a usable client. Known
Nucleus and other identified incompatible engines, missing identity, and
contradictory versions are refused and the native connection is discarded.
`endpoint_identity` records the admitted reported PostgreSQL version.
This checks reported engine identity, not authentication, TLS, feature parity,
or absence of poolers. Direct topology remains a caller declaration; provider
and pooling capability profiles require separate qualification. PostgreSQL
compatibility markers can be spoofed and are not a security boundary.
Wrapping an existing connection with `Database(connection)` or
`AsyncDatabase(connection)` is a trusted low-level path without admission
metadata (`endpoint_identity is None`).
Each Database owns one connection, with no implicit connection pool.
The startup probe uses psycopg's documented
[connection parameter status](https://www.psycopg.org/psycopg3/docs/api/objects.html#psycopg.ConnectionInfo.parameter_status).

```python
from neutron.orm import ColumnSpec, Database, Table, insert, select

users = Table("users", {
    "id": ColumnSpec(int, "int4"),
    "name": ColumnSpec(str, "text"),
}, schema="app")

with Database.connect(database_url) as db:
    with db.transaction():
        db.execute(insert(users, {"id": 1, "name": "Ada"}))
    name = db.one(select(users.column("name", str)).where(users.column("id", int).eq(1)))
```

`AsyncDatabase.connect` and all/one/one_or_none/execute/close are native awaitable
APIs. Use `async with await AsyncDatabase.connect(url)` and
`async with db.transaction()`. No hidden event-loop bridge or property-triggered
I/O exists. Concurrent active use is rejected, and a transaction remains owned
by its starting thread/task even between statements. Implicit nested transaction contexts are
unsupported; explicit savepoints are described below.

`select(column)` has a typed scalar result; `select_row(table, *columns)` returns
an explicitly selected dictionary. Nullable columns use
`table.nullable_column(name, native_type)` so their result includes None. Column
access requires declared native type agreement. This does not statically type
arbitrary dictionary writes or verify database schema against declarations.

Qualified identifiers are quoted and values are bound as native parameters.
Present false/zero/empty values remain writes. `OMIT` omits an insert field or
leaves an update field untouched; `DEFAULT` requests its database default; None
writes SQL NULL. An update containing only omitted fields refuses before SQL.
Update/delete require an explicit predicate. Predicate objects are trusted
program expressions; do not construct their SQL strings from untrusted input.

The supported metadata vocabulary is int2/int4/int8, bool, text/varchar,
numeric/Decimal, uuid/UUID, bytea/bytes, timestamp/timestamptz/datetime and
date/date. Local timestamps require naive datetime; instants require aware
datetime. Composite values and schema migrations remain unqualified; array, domain and
range profiles are described below.
Explicit JSON document support is described below. Database-generated column writes refuse. Existing canonical
schema-v2 and migration protocols are not changed by this in-memory metadata.

`one` requires exactly one result; `one_or_none` refuses multiple results.
`OrmError.sqlstate` preserves native PostgreSQL SQLSTATE; the native exception is
its cause. Native COMMIT failures carry `outcome` as aborted only for definite server
transaction rejection; connection-exception SQLSTATEs, completion-unknown errors
and unclassified failures remain indeterminate.
There is no automatic replay. Error strings omit native values, but causes can contain them: log
with appropriate redaction. User exceptions inside transactions propagate after
native rollback. PostgreSQL autocommit applies outside explicit transactions.
Cleanup failure fences and discards the native connection, including repeated
async cancellation during rollback. Closed/fenced handles reject later use.
Rows are validated against selected metadata; missing projections are not
silently zero-filled. There is no automatic retry.

Verification commands from `python/`, in an isolated remote environment:

```sh
python -m pip install -e '.[test,orm]' mypy
python -m pytest tests/test_orm_core.py tests/test_orm_clients.py tests/test_orm_types.py
python -m mypy neutron/orm tests/test_orm_types.py
NEUTRON_LIVE_REQUIRED=1 python -m pytest tests/test_orm_live.py
```

Live tests require a private `NEUTRON_TEST_DATABASE_URL` for a disposable
PostgreSQL database. Native psycopg assertions independently verify stored state;
unit tests and declared exports are not full ORM certification.

## Bounded scalar mapped lifecycle

`ModelMapping`, `Session`, `AsyncSession`, `ObjectState` and `ConflictError` provide
an initial scalar dataclass persistence lifecycle. This expands the SQL core;
it still does not establish full SQLAlchemy replacement. Inheritance and implicit
lazy I/O remain unsupported. Explicit graph, merge, bulk, event, savepoint,
instrumented expiration and mutable JSON APIs are described below.

```python
from dataclasses import dataclass
from neutron.orm import ColumnSpec, ModelMapping, Session, Table

@dataclass
class User:
    id: int | None = None
    name: str = "new"

table = Table("users", {
    "id": ColumnSpec(int, "int4", generated=True),
    "name": ColumnSpec(str, "text"),
}, schema="app")
users = ModelMapping(User, table, {
    "id": table.column("id", int),
    "name": table.column("name", str),
}, primary_key=("id",))

with Session.connect(database_url) as session:
    user = User(name="Ada")
    session.add(users, user)
    session.commit()  # flush obtains generated id through native RETURNING
    assert session.get(users, user.id) is user
    user.name = "Grace"
    session.commit()
```

Metadata does not create tables. The example requires an actual generated
integer primary key and text column. A generated field may have Optional native
type annotation and None while transient; its column metadata remains nonnullable
and generated. Generated fields must be unset on insert. Mapped dataclasses must
be mutable, support weak references, and use constructor fields; mapped
`init=False` fields and inheritance refuse. Nonfinite numeric primary keys refuse;
instant keys normalize to UTC for identity lookup. Metadata is immutable.

`get` returns the same instance for a qualified identity within one Session.
Objects cannot attach to two live Sessions. Ownership uses weak references to
Session state, with release on rollback/detach; there is no process identity cache.
Scalar changes are detected against snapshots at flush. Every pending record is
validated before the first mutation. Updates/deletes match the prior mapped
snapshot, so an external conflicting edit raises ConflictError instead of blind
replacement. This is optimistic protection for mapped columns, not universal
concurrency control for unmapped columns or trigger effects.

Context exit rolls back; only explicit commit or a successful `begin()` context
commits. Autobegin/autoflush default true; `autobegin=False` requires a begin
context. `no_autoflush()` is a synchronous scope usable in both Session variants.
Flush failures block further operations until rollback. Rollback restores tracked
snapshots and clears generated identities for new objects. Unknown commit/cleanup
outcomes mark objects indeterminate and fence the Session. Values remain loaded
on commit; callers must not assume automatic freshness against other writers.

AsyncSession.add/delete are synchronous state operations; get/flush/commit/
rollback/close and begin are native awaitable operations. Use
`async with await AsyncSession.connect(url)`. An AsyncSession belongs to its
creating task, including between statements; another task is rejected. Plain
dataclass attributes do not initiate I/O. Applications must obey ownership for
ordinary attribute mutation as well as API calls.

`Database.begin()` and `await AsyncDatabase.begin()` expose narrow lifecycle
handles with explicit commit/rollback and terminal state checks. Mutation
`.returning(column)`/`.returning_row(*columns)` produce typed results. Implicit
RETURNING execution is transactional; invalid cardinality/decoding rolls it back.
Inside an existing transaction it marks that transaction rollback-only even if
the caller catches the error. `CommitCancelledError` preserves asyncio cancellation
semantics and carries indeterminate outcome when COMMIT may already have happened.

Additional remote checks include `tests/test_orm_returning.py`,
`tests/test_orm_mapping_state.py`, and mandatory-live
`tests/test_orm_session_live.py`. Check explicit ORM modules and typed consumers;
unrelated package-root typing errors must be reported separately.

Qualified queries can join distinct physical tables with `query_from(table)`.
`inner_join(other, on=...)` and `left_join(other, on=...)` require a predicate
connecting the existing scope to the new table. `select(field(column))` returns
a scalar; `select_pair(field(first), outer_field(second))` returns a typed tuple.
Every column projected from a left join's right side requires `outer_field`,
which adds `None` to its result type. Projections use internal unique SQL aliases.
`order_by(Order(column, descending=True))`, `limit(n)`, and `offset(n)` are
immutable operations. `one` and `one_or_none` reject paginated queries.
Additional typed query algebra is described below. Arbitrary statically keyed
dictionary shapes and implicit model relation loading remain outside Query.

`Predicate(sql, params, owners)` is an explicit trusted SQL escape hatch.
Ownership checks on generated expressions do not make raw predicates a SQL sandbox.

For `json`/`jsonb`, use `ColumnSpec(JsonDocument, 'jsonb', nullable=True)`.
`JsonDocument(text)` is immutable validated JSON text; `parsed()` returns a
fresh tree with exact `Decimal` fractional numbers. `JSON_NULL` represents
JSON `null`; Python `None` represents SQL NULL. Dicts/lists/floats are not
implicitly serialized. Duplicate keys and nonfinite numbers are refused.
Native connections install a connection-local loader, preserving this
distinction without changing other psycopg connections. PostgreSQL `jsonb`
normalizes formatting/order; PostgreSQL JSON restrictions still apply and
native errors retain SQLSTATE. Mapped fields support immutable replacement
of `jsonb` documents, not in-place mutation tracking. `json` equality/mapping
and JSON primary keys are refused. JSON operators/path queries remain future work.
The adapter uses psycopg's [connection-local JSON loading and explicit wrappers](https://www.psycopg.org/psycopg3/docs/basic/adapt.html#json-adaptation).


Explicit relation reads use `Relation(parent_mapping, child_mapping,
parent_fields=('id', 'tenant'), child_fields=('parent_id', 'tenant'))`.
Keys must be identical nonnullable integer/string/bool/UUID column profiles.
`load_many(db, relation, parents, budget=LoadBudget(max_parents=100,
max_rows=1000, batch_size=20), query=relation.query().order_by(Order(child_id)))`
returns a tuple of typed `Association(parent, children)` values. Parent input
order and duplicates are retained, missing matches have empty child tuples,
and duplicate parent slots receive independently constructed child instances.
`load_one` rejects multiple matches; async clients use `async_load_many` and
`async_load_one`. `inverse()` explicitly reverses the relation.

Parent budgets count every input slot; row budgets count expanded child slots,
including duplicate parents. Key batches deduplicate input composites, bind
parameters, and refuse PostgreSQL parameter overflow before dispatch. Native
reads are limited to the remaining budget plus one overflow sentinel row.
Filters and child ordering are supported through the relation's own mapped
query. Without ordering, child order is unspecified. Global limits/offsets,
joined/replaced child projections, nullable/custom keys, per-parent pagination,
lazy loads and many-to-many inference are unsupported by these standalone
read functions. Explicit Session graph operations and identity attachment are
described below. Multiple batches have no implicit shared snapshot;
callers must arrange RepeatableRead/Serializable isolation when needed. Parent
objects are never modified, and failures return no partial association result.

`session.refresh(obj, discard_changes=False)` (awaited for AsyncSession) performs
an explicit exact-one read onto the same tracked object, without autoflush.
Only existing persistent objects are eligible; pending/newly flushed inserts,
deleted/detached objects and primary-key mutations are refused. Dirty scalar
fields require `discard_changes=True`. All returned values and the unchanged
canonical key are validated before in-place replacement; generated fields come
from the database. Missing/multiple rows, decoding failures or cancellation
require rollback. Refresh preserves the pretransaction original snapshot:
rollback restores it, which may be stale after external writes; explicitly
refresh again to observe current state. Commit adopts the refreshed baseline.

`session.detach(obj)` is synchronous for both Session classes and performs no
I/O. It requires a clean existing persistent object, no active transaction and
no uncommitted snapshot changes. It removes the identity entry, releases object
ownership and records `DETACHED`; later `get` loads a separate instance. `add`
continues to mean INSERT, not attach-existing. This API does not implement
expiration, implicit attribute I/O or graph reconciliation.

`session.attach_existing(mapping, obj, discard_changes=False)` (awaited for
AsyncSession) verifies a complete primary key against exactly one native row,
without INSERT, upsert or autoflush, then tracks the same object. Detached state
alone does not prove a baseline. By default every supplied scalar must match
the fetched row: instants compare in UTC, numeric values stay exact and JSONB
compares structurally without equating booleans with numbers. A mismatch raises
`ConflictError`, leaves the object unmodified, and requires rollback.
`discard_changes=True` explicitly replaces caller values with the database row.
The authoritative fetched row becomes both baseline and rollback original:
rollback retains persistent ownership and does not recover discarded caller
changes. Another owner or cached object at that identity refuses; ownership is
rechecked after I/O before mutation. Generated fields come from the database;
PK mutation during the read refuses. Detached edits use the explicit baseline-checked merge API described below.

The scalar mapping profile admits ordinary mutable dataclass attributes and
standard weak-reference-capable slots. Custom attribute access/mutation hooks,
field properties/descriptors and custom metaclasses are refused at mapper
admission; snapshot/construct/restore recheck this profile. Model definitions
must remain stable after mapping. This restriction also protects get, flush,
refresh and attach-existing restoration; custom model instrumentation/hooks
require a separately designed lifecycle profile.

`with db.stream(query, batch_size=256) as rows` and
`async with db.stream(query, batch_size=256) as rows` use native named PostgreSQL
server cursors. `query` must be a Select or typed Query read projection; batch
sizes are integers from 1 through 10000. Iterators retain their projected type
and the existing exact Decimal, aware-time and SQL-NULL/JsonDocument codecs.
One batch is buffered; fetching the entire result is never implicit. A stream
leases the connection to its owning thread/task, including while buffered rows
remain, and blocks other ORM operations and transaction-handle settlement.

A stream outside a transaction owns a READ ONLY transaction. Exhausting the
iterator through its final empty fetch commits on clean context exit; early
exit or failure rolls back. A stream within an existing transaction closes
only its cursor on clean exit, preserving the caller's transaction modes.
Failures poison that transaction and require rollback. Explicit context exit
is required after an early break; no generator finalizer or implicit close
promise is made. Escaped iterators refuse after scope settlement.

Native async Task cancellation and sync KeyboardInterrupt trigger cleanup;
custom deadline/token cancellation is unsupported. Cursor/transaction cleanup
is bounded to five seconds across close and settlement independently of the original caught
cancellation; timeout or repeated cancellation fences the connection before
reuse. COMMIT cancellation retains the existing indeterminate-outcome error.
Raw Predicate SQL remains a trusted escape hatch, not a SQL sandbox; the owned
READ ONLY transaction is enforced by PostgreSQL. Arbitrary custom connection
implementations are outside the native-driver cleanup bound.


Scalar Session callbacks are registered in order with `session.listen(name,
callback)`. `SessionEvent` carries the event name and affected object (or None
for flush/settlement events). Supported names are `before_flush`, `before_insert`,
`before_update`, `before_delete`, `after_flush`, `after_commit`, and
`after_rollback`. Sync callbacks must return None; AsyncSession accepts native
awaitable callbacks and awaits each in registration order. All callbacks for
planned writes run before the final full-record validation and first SQL write.
Scalar attributes may be edited in prewrite callbacks. Session API reentrancy,
listener registration and close from callbacks refuse. Failure before commit
requires rollback and emits no after_commit event. An after_commit callback runs
only after a known successful commit and adopted object baseline; its exception
cannot undo that commit. after_rollback runs after explicit reconciliation.
Callbacks have no exactly-once external side-effect guarantee: use an outbox for
external work. Relationship/attribute instrumentation is not provided by these
scalar callbacks.

`alias(table, name)` creates a distinct read source for self joins; alias columns
are owned independently of the physical table. Aliases refuse mutation and mapped
persistence. Scalar subqueries use `in_query(column, query)`; EXISTS predicates
use `exists(query)`. Correlated subqueries must explicitly declare outer sources
with `query_from(inner).correlate(outer)` before referencing their columns.
The enclosing query validates that each declared outer source belongs to its
scope. Native types for IN projections must agree exactly. Alias labels are
quoted and all subquery values remain bound in SQL occurrence order. CTE/derived
sources and universal SQL expressions remain separate requirements.

Typed query projections also admit `count(column)`, `sum_value(column)`,
`avg(column)`, `min_value(column)` and `max_value(column)`. COUNT decodes int8;
SUM(int2/int4) decodes int8 and SUM(int8/numeric) decodes Decimal. Because runtime
column profiles do not encode integer widths in Python's type parameter,
sum_value has static result `int | Decimal | None`; AVG is Decimal or None.
Empty aggregates preserve NULL except COUNT's zero. Unsupported native profiles
refuse before SQL. `group_by(*columns)` validates owned grouped projections;
`having(aggregate.gt(value))` binds the predicate value. `distinct()` requests
row DISTINCT. `row_number(table, partition_by=(...), order_by=(Order(...),))`
produces an int8 window projection with validated source ownership.

`union(other, all=False)`, `intersect(other)` and `except_(other)` require identical
projection arity/native result profiles, retain bound parameter order, and
compose from left to right. Apply final limit/offset after composition; exact-one
APIs reject pagination as usual. Set-result ordering by output labels and broader
aggregate/window expression families require a subsequent API; raw source
ordering over a set result refuses.

Read queries become owned projected sources with `derived(query, name,
labels=('id', ...))` or `cte(query, name, labels=...)`. Labels must uniquely cover
all projected fields; callers retrieve columns with their declared native type
and nullability. Derived/CTE writes and mapped persistence refuse. Sources must
be uncorrelated: lateral sources and recursive CTEs require separate contracts.
Their binds precede enclosing WHERE binds in SQL occurrence order. CTE source
columns are explicitly named, preventing internal projection aliases leaking
into the consumer's metadata.

Raw Mutation and compiled read/RETURNING SQL pass conservative single-statement
admission globally; owned transactions permit only data/query statements. Transaction/session control commands
are refused; comments, quoted strings/identifiers, dollar strings and trailing
semicolons are scanned without treating embedded text as commands. Ambiguous
ordinary-string backslashes refuse. Parameterized single data/query statements
remain supported. This contains direct state-machine escapes; trusted SQL
functions and predicates are not a security sandbox.

Session `add_graph(relation, parent, children)` supports explicit insert graphs
whose relation references the parent's complete declared primary key. Child FK
fields may start unset; flush resolves generated parent keys, orders dependent
inserts and validates every other field before any SQL write. `link(relation,
parent, child)` connects tracked pending/persistent records. Overlapping child
FK ownership and generated FK targets refuse. Cyclic insert dependencies refuse
before SQL; deferred-cycle execution is not certified. All graph statements share
the Session transaction; rollback restores generated identities and original FK
values. Database uniqueness/FK errors remain native failures requiring rollback.
Implicit graph discovery, ownership cascades, nullable disconnects and automatic
association deletion remain separate requirements.

`session.load_relation(relation, parents, budget=...)` is a bounded explicit
select-in read for this Session's tracked parents. AsyncSession requires await.
Loaded children attach to its identity map, so duplicate parent slots and repeated
loads reuse the same tracked child object. Existing cached values are not silently
refreshed; use explicit refresh. `singular=True` refuses multiple matches.
No relationship properties or hidden lazy I/O are installed.

`ModelMapping(..., version_field='version')` opts into an application integer
version column, outside the primary key and nonnullable/nongenerated. Dirty
updates increment it with range validation before SQL; optimistic predicates
include the prior version and scalar snapshot. Stale zero-row updates raise
ConflictError. Native RETURNING adopts the new version, and rollback restores
its prior value. Direct application edits to a tracked version refuse. This
supports integer versions, not universal server-generated/concurrent bulk tokens.

Session `bulk_update(mapping, values, where=...)` and `bulk_delete(mapping,
where=...)` use an explicit native RETURNING synchronization boundary. They
flush pending scalar changes first, then refresh affected tracked baselines or
mark deleted objects inside the same transaction. Rollback restores original
object state; commit removes deleted identities. Untracked returned rows are
not implicitly attached. Primary-key edits refuse. Versioned bulk updates
increment integer tokens in PostgreSQL and adopt returned tokens. Per-row
callbacks are not emitted for bulk operations; these APIs explicitly bypass
ordinary per-object optimistic matching and use the caller's predicate. Results
are buffered, so this is a bounded-use API, not a streaming large-table update
claim. AsyncSession requires await.

`session.merge(mapping, detached_obj, expected=baseline)` applies a complete
scalar detached edit onto the Session's authoritative existing identity and
returns that tracked target; it does not attach the source object or insert a
missing row. The complete expected baseline must match a fresh native read,
including composite identity and optional version. Dirty/uncommitted cached
targets, attached sources, generated-field edits and key/version patches refuse.
This makes stale detached edits explicit conflicts. Flush performs the ordinary
optimistic write; rollback restores the fetched/pretransaction target baseline
while leaving the caller's detached edit intact. AsyncSession requires await.
This explicit scalar merge contract does not implement SQLAlchemy graph merge
or inference of an absent baseline.

`with session.savepoint()` and `async with session.savepoint()` flush existing
work before creating a native PostgreSQL savepoint and a complete scalar object
checkpoint. Successful exit flushes changes before releasing that savepoint. A body/flush failure rolls back that savepoint, restores saved
values/baselines/states, clears identities created inside it and permits the
outer transaction to continue. Nested savepoints are explicit. Session and raw
transaction-handle commit/rollback/close refuse while a savepoint is open.
Cleanup failure fences the connection and marks tracked outcomes indeterminate;
no partial checkpoint is advertised as reconciled. The existing plain
`transaction()` context still refuses implicit nested transactions.

`ModelMapping(..., instrumented=True)` installs finite library-owned scalar
field descriptors on the validated dataclass (ordinary attributes or native
weak-reference-capable slots). `session.expire(obj, *fields)` marks an existing
clean object EXPIRED; omitting fields expires all mapped fields. Dirty state
requires explicit `discard_changes=True`. Expired property reads and writes
raise ExpiredAttributeError without network I/O, including in AsyncSession.
Explicit `refresh(obj)` or `get(mapping, key)` reloads the same identity; async
callers await them. Rollback restores loaded pretransaction scalar values;
savepoint rollback restores its checkpoint's expiration flags. Expired flags
are weakly retained with the object, preventing detached expired objects from
silently exposing a stale value. `attach_existing(..., discard_changes=True)`
can explicitly rehydrate such a detached object.

`Session(..., expire_on_commit=True)` and AsyncSession opt into expiration after
known commits, requiring instrumented mappings. The default remains False for
compatibility with ordinary dataclass mappings. Custom descriptors/setters and
implicit lazy loading are still refused; library instrumentation does not claim
mutable JSON/collection or relationship-property tracking.

`ColumnSpec(MutableJson, 'jsonb', nullable=True)` opts into explicit nested mutable
collection tracking. `MutableJson(tree).value` holds dict/list/native JSON
scalars; integers and Decimal numbers serialize exactly, floats/nonfinite values,
nonstring object keys and cycles refuse. `MutableJson(None)` means JSON null;
Python None means SQL NULL. Metadata converts the native immutable JSON document
into this declared codec without changing other connection loaders. Compiled
binds snapshot the document, and mapped snapshots/refresh/restore/merge deep-copy
trees. Nested dict/list edits are detected at flush, including callback edits;
rollback/savepoints replace them with independent baseline copies. Retained old
tree references are not a persistent object handle after restoration. Plain
unwrapped dict/list fields and PostgreSQL array/range collection codecs remain
outside this JSONB profile.

Post-flush callbacks may observe state but cannot leave additional unflushed
edits: those refuse before COMMIT and require rollback. Failure in a postcommit
notification carries explicit known `outcome='committed'` through PostCommitError,
PostCommitCancelledError or PostCommitInterruptedError. Async cancellation and
KeyboardInterrupt retain their respective exception families; the adopted
committed baseline is never presented as rolled back.

`OwnedRelation(..., on_delete='restrict'|'delete'|'nullify', orphan_delete=False)`
adds explicit ORM ownership policy. Parent keys cover their complete primary
identity; nullify requires nullable non-primary child FK fields.
`session.disconnect(relation, parent, child)` clears those keys explicitly.
`remove_related` deletes the child only when orphan_delete is declared, otherwise
uses nullable disconnect. `delete_graph(relation, parent, budget=...)` loads and
validates owned children, orders delete/nullify actions before the parent, and
preserves ordinary optimistic snapshots and rollback. Async deletion is awaited.
Pass `descendants=(owned_child_relation, ...)` to register deeper ownership.
Discovery and restrict admission finish before deletion/nullification marking.
The same LoadBudget limits total traversed deletion objects (`max_parents`) and
expanded relation rows (`max_rows`) across all levels; `max_depth=32` limits
edge depth, with root at depth zero; accepted depth limits stay below 256. Repeated/cyclic ownership and conflicting
delete/nullify paths refuse. Only delete policies traverse descendants; nullify
preserves their children. Budget failures require rollback, preserve persistent
object states, and produce no graph deletion SQL.
These are ORM actions independent of database ON DELETE clauses: concurrent
unloaded children remain protected by the database FK, causing safe transaction
failure rather than silently certifying an incomplete graph. Undeclared relation
levels and many-to-many links are not inferred.

`ManyToMany(parent_relation, target_relation)` declares an explicit through
mapper shared by both relations. Both endpoints reference complete primary
identities, and the through primary key covers all association FK fields.
`connect_many_to_many(meta, parent, target, association)` attaches the supplied
through object and orders generated endpoint identities before its INSERT.
Shared composite tenant fields must agree; no last-write-wins overwrite is
allowed. `disconnect_many_to_many` deletes only the exact tracked through row,
preserving both targets. Duplicate associations remain database uniqueness
errors with complete rollback. These state operations are synchronous for both
Session families; flush/commit are native sync/async as usual. Association payload
fields use ordinary mapped tracking/hooks, and explicit load_relation reads reuse
through-object identity. Surrogate through keys without declared composite
uniqueness and automatic target collection inference remain unsupported.

Public Database SQL APIs globally refuse raw BEGIN/COMMIT/ROLLBACK/SAVEPOINT and
session control, even outside an owned scope. Standalone supported single DDL
statements remain available through Mutation outside a transaction; raw batches
and procedural statements are not admitted. Root transaction entry requires an
IDLE native connection with autocommit=True, preventing externally opened native
transactions from being mistaken for an owned root. Borrowed constructors still
skip endpoint identity admission, but callers must settle external transactions
and configure autocommit before using ORM root lifecycle APIs.

`array_spec(int, 'int4', nullable=True)` declares an immutable `PgArray[int]`
column. `PgArray(dimensions=(ArrayDimension(length=2, lower_bound=-3),),
elements=(1, None))` retains flat row-major members and native dimensions/lower
bounds. SQL NULL is None; an empty array is `PgArray((), ())`; NULL members stay
inside elements. Up to six positive dimensions and one million members are
admitted. Array profiles currently cover the qualified builtin scalar families,
including text, exact numeric, UUID, bytes, finite date/timestamps, time and
interval. Nested arrays use dimensions rather than nested PgArray objects;
list/dict elements and unknown types refuse. `table.column('data', PgArray[int])`
retains the declared element type for static consumers. Mapped dataclass fields
must declare PgArray with its element type; arrays are replaced explicitly, and
immutable snapshots preserve rollback. Array primary identities remain refused.

`ColumnSpec(TimeOfDay, 'time')` preserves microseconds since midnight, including
`TimeOfDay(86_400_000_000)` for 24:00:00. It has no timezone. `ColumnSpec(Interval,
'interval')` preserves `Interval(months, days, microseconds)` components without
converting months into fixed durations. Date remains Python datetime.date with
finite years 1–9999; timestamps retain existing local/instant modes. Infinity,
unsupported timetz and unknown type profiles refuse.

Native connect APIs register component codecs only on the new connection and
use binary result cursors, preserving array bounds and temporal components
independently of server formatting settings. Compiler projections carry exact
builtin SQL type OIDs, verified against native result descriptions before
objects are decoded; binary arrays additionally verify their element OID.
Qualified user enum/domain types require explicit catalog admission and are not
silently decoded as builtin strings or scalars.

`range_spec(int, 'int4range', nullable=True)` declares a `PgRange[int]` column.
`PgRange(lower, upper, lower_inclusive=False, upper_inclusive=False)` preserves
finite bound values and inclusivity; None at a bound is unbounded, while None at
the column is SQL NULL. `PgRange(empty=True)` is a distinct empty range. Empty
ranges cannot carry bounds and unbounded endpoints cannot be inclusive.
Builtin int4/int8/numeric/date/local timestamp/instant timestamp ranges have
explicit native subtype OIDs. Discrete PostgreSQL ranges can canonicalize writes:
for example `[1,3]` returns `[1,4)`, and mapped RETURNING adopts that native value.
Range values are immutable and replacement edits use normal snapshot rollback;
range primary identities and multiranges remain unqualified.

Qualified enum/domain admission is explicit native I/O. `db.enum_spec(schema,
name)` returns `ColumnSpec[PgEnum]`; `db.domain_spec(schema, name,
ColumnSpec(Decimal, 'numeric'))` returns `ColumnSpec[PgDomain[Decimal]]` after
following a bounded catalog domain chain and verifying its exact base OID.
Async callers await these methods. Native enum labels, including empty strings,
use `PgEnum(label, spec.native_type)`; domain values use
`PgDomain(value, spec.native_type)`. The immutable CatalogType identity retains
qualified namespace/name, original OID, effective base OID and owning client.
The database enforces enum membership and domain constraints on native writes.

Use `db.catalog_table(name, columns, schema=...)` to verify actual physical
column OIDs and declared nonnullable profiles. Ordinary Table construction
refuses enum/domain metadata lacking this admission. Domain result descriptions
can expose their base OID; their original domain identity comes from the exact
catalog column check, and decoding wraps the base value with that identity.
Precision is retained through the declared base codec. Wrong schemas/types,
unknown composites, incompatible bases and connection reuse of another client's
catalog metadata refuse. Query aliases/derived sources and explicit subqueries
retain this ownership. Catalog admission is a point-in-time contract: rebuild
metadata after DDL. A Session can share the admitted Database via `Session(db)`
or AsyncSession(db); metadata admission remains outside automatic flush/property
access. User multirange/composite adapters remain unqualified. Scalar domain primary
identities retain their qualified tags and normalize underlying instant keys to
UTC; nonfinite numeric keys refuse. PostgreSQL interval equality can equate
different month/day representations, so interval, array, range and JSON primary
profiles remain refused, including domains over those profiles. Domains over an
explicit admitted enum base retain both domain and enum identities. Matching
qualified enum/domain integer/string/bool/UUID keys can participate in explicit
relation metadata; incompatible type identities refuse.

`PolymorphicMapping(Base, table, all_field_columns, primary_key=('id',),
discriminator='kind', variants={'base': Base, 'cat': Cat, 'dog': Dog})` declares a
single-table dataclass hierarchy explicitly. Every variant owns one unique text
discriminator. Base fields/identity/version are shared; subtype-only columns must
be nullable and nongenerated, while each class's nonoptional active fields remain
required. Native unknown tags, inactive column values and constructor coercion
refuse. Classes choose their own matching discriminator defaults or supply them
explicitly; the mapper does not change classes or tags to coerce an object.

One family owns the table's Session identity. `session.get(family, key)` returns
the registered subclass. `session.get(family.subtype(Cat), key)` is statically
Cat|None, adds an exact discriminator SQL predicate and reuses that same object.
`select_polymorphic(family_or_subtype, where=..., max_rows=1000)` performs a bounded
mapped query with the same identity reuse; async methods are awaited. Family
flushes retain ordinary generated keys, versions, events, rollback/savepoints,
explicit library instrumentation and expiration. Readonly discriminator changes
and discriminator bulk/merge patches refuse. Native subtype changes under an
already tracked identity require a new Session; no in-place class replacement is
performed. Ordinary independent same-table mappers still conflict.

The selected capability is single-table discriminator mapping. Joined/concrete
inheritance, implicit subtype discovery, arbitrary descriptors and third-party
mapper plugins remain unsupported; it does not establish complete SQLAlchemy
replacement. The authored conversion fixture at
`conformance/polyglot/applications/python/polymorphic_sti.py` pins SQLAlchemy
2.1.3 and compares independent native rows, base/subtype identity reuse, filtering
and rollback. It is an authored application fixture, not a claimed external app.

`Inet.parse('192.168.1.73/24')` preserves the address's host bits and prefix;
`CIDR.parse('192.168.1.0/24')` requires a network address and rejects host bits.
Both immutable values support IPv4 and IPv6, including IPv4-mapped IPv6 without
changing families. Scoped IPv6 addresses refuse. `ColumnSpec(Inet, 'inet')` and
`ColumnSpec(CIDR, 'cidr')` bind and decode native binary address components with
exact builtin OIDs and header-family checks. SQL NULL remains `None`; `/0` and
all-zero addresses remain actual values. Mapped replacement, identity reads,
rollback, streaming and async APIs use the same component contracts.
