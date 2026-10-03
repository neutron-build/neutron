# Opt-in PostgreSQL SQL core

Install `neutron-framework[orm]`. Import from `neutron.orm`. Existing
`neutron.nucleus` asyncpg clients are unchanged.

This is a bounded native psycopg synchronous/asynchronous SQL core with
explicit scalar dataclass Sessions. Associated writes, cascades and full SQLAlchemy parity
remain outside this slice. Only `postgres-direct` is admitted.
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
by its starting thread/task even between statements. Nested transactions are
explicitly unsupported in this slice.

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
datetime. Arrays/domains/composites/JSON/ranges and schema migrations are
unsupported here. Database-generated column writes refuse. Existing canonical
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
it still does not establish full SQLAlchemy replacement. Relations, inheritance,
mutable JSON/collections, lazy/expired properties, merge, bulk synchronization,
hooks, savepoints and automatic expiration on commit remain unsupported.

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
Self joins, aliases, arbitrary row shapes, aggregates, subqueries and relation
loading are outside this bounded query API.

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
lazy loads, many-to-many inference, graph writes, cascades and Session identity
attachment are unsupported. Multiple batches have no implicit shared snapshot;
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
PK mutation during the read refuses. Patch merging remains unsupported.

The scalar mapping profile admits ordinary mutable dataclass attributes and
standard weak-reference-capable slots. Custom attribute access/mutation hooks,
field properties/descriptors and custom metaclasses are refused at mapper
admission; snapshot/construct/restore recheck this profile. Model definitions
must remain stable after mapping. This restriction also protects get, flush,
refresh and attach-existing restoration; custom model instrumentation/hooks
require a separately designed lifecycle profile.
