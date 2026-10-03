# Opt-in PostgreSQL SQL core

Install `neutron-framework[orm]`. Import from `neutron.orm`. Existing
`neutron.nucleus` asyncpg clients are unchanged.

This is a bounded native psycopg synchronous/asynchronous SQL core. It does not
implement mapped Session identity, dirty tracking, flush, associations or a full
SQLAlchemy replacement. Only the explicit PostgreSQL direct profile is admitted.
It owns one connection per Database instance, with no implicit connection pool.

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
