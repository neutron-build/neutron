"""Neutron ORM persistence boundary for the converted subset of the template backend.

Derived from fastapi/full-stack-fastapi-template (MIT, Copyright (c) 2019 Sebastian
Ramirez; the LICENSE file stays in the prepared tree). The SQLModel table classes in
app.models are left untouched and are no longer used for persistence; these mapped
dataclasses and tables replace them. Relationships are not mapped: items are reached
through the owner_id column only.
"""
import asyncio
import functools
import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Callable, TypeVar

from neutron.orm import (
    ColumnSpec,
    Database,
    ModelMapping,
    Mutation,
    Order,
    Predicate,
    Query,
    Session,
    Table,
    count,
    query_from,
    select,
)
from neutron.orm import field as projection

from app.core.config import settings

M = TypeVar("M")
T = TypeVar("T")

# The ORM qualifies every table name, so the schema comes from the environment by name.
SCHEMA = os.environ.get("NEUTRON_PYAPP_SCHEMA", "public")


def get_datetime_utc() -> datetime:
    return datetime.now(UTC)


@dataclass
class User:
    email: str
    hashed_password: str
    is_active: bool = True
    is_superuser: bool = False
    full_name: str | None = None
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    created_at: datetime | None = field(default_factory=get_datetime_utc)


@dataclass
class Item:
    title: str
    owner_id: uuid.UUID
    description: str | None = None
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    created_at: datetime | None = field(default_factory=get_datetime_utc)


# Column profiles follow the final Alembic schema (user.email/full_name/item.title/
# description are varchar(255), user.hashed_password is varchar, created_at is
# nullable timestamptz); the harness verifies them against the catalog.
USER_TABLE = Table(
    "user",
    {
        "id": ColumnSpec(uuid.UUID, "uuid"),
        "email": ColumnSpec(str, "varchar"),
        "is_active": ColumnSpec(bool, "bool"),
        "is_superuser": ColumnSpec(bool, "bool"),
        "full_name": ColumnSpec(str, "varchar", nullable=True),
        "hashed_password": ColumnSpec(str, "varchar"),
        "created_at": ColumnSpec(datetime, "timestamptz", nullable=True),
    },
    schema=SCHEMA,
)
ITEM_TABLE = Table(
    "item",
    {
        "id": ColumnSpec(uuid.UUID, "uuid"),
        "title": ColumnSpec(str, "varchar"),
        "description": ColumnSpec(str, "varchar", nullable=True),
        "owner_id": ColumnSpec(uuid.UUID, "uuid"),
        "created_at": ColumnSpec(datetime, "timestamptz", nullable=True),
    },
    schema=SCHEMA,
)

USER_MAPPING = ModelMapping(User, USER_TABLE, dict(USER_TABLE.columns), primary_key=("id",))
ITEM_MAPPING = ModelMapping(Item, ITEM_TABLE, dict(ITEM_TABLE.columns), primary_key=("id",))
MAPPINGS: dict[type, ModelMapping[Any]] = {User: USER_MAPPING, Item: ITEM_MAPPING}

USER_ID = USER_TABLE.column("id", uuid.UUID)
USER_EMAIL = USER_TABLE.column("email", str)
USER_CREATED_AT = USER_TABLE.nullable_column("created_at", datetime)
ITEM_ID = ITEM_TABLE.column("id", uuid.UUID)
ITEM_OWNER_ID = ITEM_TABLE.column("owner_id", uuid.UUID)
ITEM_CREATED_AT = ITEM_TABLE.nullable_column("created_at", datetime)


def database_url() -> str:
    # Settings rewrites the scheme to SQLAlchemy's driver form; psycopg wants the plain URL.
    return str(settings.DATABASE_URL).replace("postgresql+psycopg://", "postgresql://", 1)


def all_rows(table: Table) -> Predicate:
    return Predicate("TRUE", (), frozenset({table}))


def rows_query(table: Table) -> Query[dict[str, Any]]:
    """Every mapped column of one table, decoded to a dictionary keyed by column name."""
    names = tuple(table.columns)
    fields = tuple(projection(table.columns[name]) for name in names)

    def decode(row: Any) -> dict[str, Any]:
        return {name: item.decode(row[f"p{index}"]) for index, (name, item) in enumerate(zip(names, fields))}

    return Query(query_from(table), fields, decode)


def newest_first(column: Any) -> Order:
    # PostgreSQL's own DESC null placement, as in the template's ORDER BY ... DESC.
    return Order(column, descending=True, nulls_first=True)


def count_query(table: Table, id_column: Any) -> Query[int]:
    return query_from(table).select(count(id_column))


class DbSession:
    """One connection and one mapped Session sharing it.

    A Session has no public predicate reader, so predicate/ordered/paginated reads go
    through the Database the Session shares (documented Session(db) pattern). Reads flush
    pending Session work first. Use only on the thread that opened it: the ORM's
    synchronous Session is thread-owned. fresh_reads (used by the test fixtures) re-reads
    tracked objects so a long-lived Session observes other connections' commits.
    """

    def __init__(self, database: Database, *, fresh_reads: bool = False) -> None:
        self.database = database
        self.orm = Session(database)
        self.fresh_reads = fresh_reads

    @classmethod
    def open(cls, *, fresh_reads: bool = False) -> "DbSession":
        return cls(Database.connect(database_url()), fresh_reads=fresh_reads)

    def get(self, model: type[M], key: uuid.UUID) -> M | None:
        mapping = MAPPINGS[model]
        if self.fresh_reads:
            self.orm.flush()
            column = mapping.table.column("id", uuid.UUID)
            if self.database.one_or_none(select(column).where(column.eq(key))) is None:
                return None
        found = self.orm.get(mapping, key)
        if self.fresh_reads and found is not None:
            self.orm.refresh(found)
        return found

    def add(self, obj: object) -> None:
        self.orm.add(MAPPINGS[type(obj)], obj)

    def delete(self, obj: object) -> None:
        self.orm.delete(obj)

    def bulk_delete(self, model: type, where: Predicate) -> int:
        return self.orm.bulk_delete(MAPPINGS[model], where=where)

    def commit(self) -> None:
        self.orm.commit()

    def refresh(self, obj: object) -> None:
        self.orm.refresh(obj)

    def all(self, query: Any) -> list[Any]:
        self.orm.flush()
        return self.database.all(query)

    def one(self, query: Any) -> Any:
        self.orm.flush()
        return self.database.one(query)

    def one_or_none(self, query: Any) -> Any:
        self.orm.flush()
        return self.database.one_or_none(query)

    def execute(self, mutation: Mutation) -> int:
        self.orm.flush()
        return self.database.execute(mutation)

    def close(self) -> None:
        try:
            self.orm.close()
        finally:
            self.database.close()


# Sync Sessions are thread-owned, and FastAPI may run a request's dependencies and
# endpoint on different worker threads. Every DbSession call made by the app therefore
# runs on this one thread, which also keeps the event loop free of blocking I/O.
_DB_THREAD = ThreadPoolExecutor(max_workers=1, thread_name_prefix="neutron-orm")


async def run_db(function: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    return await asyncio.get_running_loop().run_in_executor(_DB_THREAD, functools.partial(function, *args, **kwargs))
