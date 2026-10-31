"""Public APIs only; no provider normalization or oracle in the timed methods."""
from __future__ import annotations

from contextlib import asynccontextmanager
from decimal import Decimal

import asyncpg
from pydantic import BaseModel, ConfigDict
from sqlalchemy import BigInteger, ForeignKeyConstraint, Integer, LargeBinary, Numeric, String, delete, event, insert, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, selectinload

from neutron.nucleus.client import NucleusClient


class Document(BaseModel):
    model_config = ConfigDict(strict=True)
    tenant: str
    id: int
    project_id: int
    version: int
    amount: Decimal
    note: str | None
    payload: bytes


class Project(BaseModel):
    model_config = ConfigDict(strict=True)
    tenant: str
    id: int
    title: str


class Base(DeclarativeBase):
    pass


class SAProject(Base):
    __tablename__ = "projects"
    tenant: Mapped[str] = mapped_column(String, primary_key=True)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    title: Mapped[str] = mapped_column(String)
    documents: Mapped[list[SADocument]] = relationship(order_by="SADocument.id", lazy="raise")


class SADocument(Base):
    __tablename__ = "documents"
    __table_args__ = (ForeignKeyConstraint(["tenant", "project_id"], ["projects.tenant", "projects.id"]),)
    tenant: Mapped[str] = mapped_column(String, primary_key=True)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    project_id: Mapped[int] = mapped_column(Integer)
    version: Mapped[int] = mapped_column(BigInteger)
    amount: Mapped[Decimal] = mapped_column(Numeric(40, 18, asdecimal=True))
    note: Mapped[str | None] = mapped_column(String, nullable=True)
    payload: Mapped[bytes] = mapped_column(LargeBinary)


FIELDS = "tenant,id,project_id,version,amount,note,payload"
POINT = f"SELECT {FIELDS} FROM documents WHERE tenant=$1 AND id=$2"
PAGE = f"SELECT {FIELDS} FROM documents WHERE tenant=$1 AND id>$2 ORDER BY id LIMIT 20"
CHILDREN = f"SELECT {FIELDS} FROM documents WHERE tenant=$1 AND project_id=$2 ORDER BY id LIMIT 20"
CREATE = "INSERT INTO documents VALUES($1,$2,$3,$4,$5,$6,$7)"
CAS = "UPDATE documents SET version=version+1,note=$4 WHERE tenant=$1 AND id=$2 AND version=$3"


def params(row):
    return tuple(row[k] for k in ("tenant", "id", "project_id", "version", "amount", "note", "payload"))


class Native:
    name = "neutron-native-pydantic"

    @classmethod
    async def open(cls, url, audit=None):
        self = cls()
        self.db = await NucleusClient.connect(url, min_size=4, max_size=4)
        if audit is not None:
            held = [await self.db.pool.acquire() for _ in range(4)]
            for conn in held:
                conn.add_query_logger(lambda record: audit.append(record.query))
            for conn in held:
                await self.db.pool.release(conn)
        return self

    async def point(self, tenant, id):
        return await self.db.sql.query_one_or_none(Document, POINT, tenant, id)

    async def page(self, tenant, after):
        return await self.db.sql.query(Document, PAGE, tenant, after)

    async def children(self, tenant, id):
        return await self.db.sql.query(Document, CHILDREN, tenant, id)

    async def relation(self, tenant, id):
        project = await self.db.sql.query_one_or_none(Project, "SELECT tenant,id,title FROM projects WHERE tenant=$1 AND id=$2", tenant, id)
        return None if project is None else (project, await self.children(tenant, id))

    async def create(self, row):
        return await self.db.sql.execute(CREATE, *params(row))

    async def remove(self, tenant, id):
        return await self.db.sql.execute("DELETE FROM documents WHERE tenant=$1 AND id=$2", tenant, id)

    async def cas(self, tenant, id, expected, note):
        return await self.db.sql.execute(CAS, tenant, id, expected, note)

    async def rolled_back_create(self, row):
        async with self.db.transaction() as tx:
            await tx.sql.execute(CREATE, *params(row))
            raise RollbackProbe()

    async def close(self):
        await self.db.close()


class Raw:
    name = "raw-asyncpg"

    @classmethod
    async def open(cls, url, audit=None):
        self = cls()
        async def initialize(conn):
            if audit is not None:
                conn.add_query_logger(lambda record: audit.append(record.query))
        self.pool = await asyncpg.create_pool(url, min_size=4, max_size=4, init=initialize)
        return self

    async def point(self, tenant, id):
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(POINT, tenant, id)

    async def page(self, tenant, after):
        async with self.pool.acquire() as conn:
            return await conn.fetch(PAGE, tenant, after)

    async def children(self, tenant, id):
        async with self.pool.acquire() as conn:
            return await conn.fetch(CHILDREN, tenant, id)

    async def relation(self, tenant, id):
        async with self.pool.acquire() as conn:
            project = await conn.fetchrow("SELECT tenant,id,title FROM projects WHERE tenant=$1 AND id=$2", tenant, id)
        return None if project is None else (project, await self.children(tenant, id))

    async def create(self, row):
        async with self.pool.acquire() as conn:
            return int((await conn.execute(CREATE, *params(row))).split()[-1])

    async def remove(self, tenant, id):
        async with self.pool.acquire() as conn:
            return int((await conn.execute("DELETE FROM documents WHERE tenant=$1 AND id=$2", tenant, id)).split()[-1])

    async def cas(self, tenant, id, expected, note):
        async with self.pool.acquire() as conn:
            return int((await conn.execute(CAS, tenant, id, expected, note)).split()[-1])

    async def rolled_back_create(self, row):
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(CREATE, *params(row))
                raise RollbackProbe()

    async def close(self):
        await self.pool.close()


class SQLAlchemy:
    orm = False
    name = "sqlalchemy-core"

    @classmethod
    async def open(cls, url, audit=None):
        self = cls()
        self.engine = create_async_engine(url.replace("postgresql://", "postgresql+asyncpg://", 1).replace("postgres://", "postgresql+asyncpg://", 1), pool_size=4, max_overflow=0, echo=False)
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)
        if audit is not None:
            @event.listens_for(self.engine.sync_engine, "before_cursor_execute")
            def observe(conn, cursor, statement, parameters, context, executemany):
                audit.append(statement)
        # Warm all four physical connections outside correctness/timing.
        held = [await self.engine.connect() for _ in range(4)]
        for conn in held:
            await conn.close()
        return self

    @asynccontextmanager
    async def reader(self):
        if self.orm:
            async with self.sessions() as conn:
                yield conn
        else:
            async with self.engine.connect() as conn:
                yield conn

    async def read(self, statement, one=False):
        async with self.reader() as conn:
            result = await conn.execute(statement)
            rows = result.scalars() if self.orm else result.mappings()
            return rows.one_or_none() if one else rows.all()

    def doc(self):
        return SADocument if self.orm else SADocument.__table__

    async def point(self, tenant, id):
        return await self.read(select(self.doc()).where(SADocument.tenant == tenant, SADocument.id == id), True)

    async def page(self, tenant, after):
        return await self.read(select(self.doc()).where(SADocument.tenant == tenant, SADocument.id > after).order_by(SADocument.id).limit(20))

    async def children(self, tenant, id):
        return await self.read(select(self.doc()).where(SADocument.tenant == tenant, SADocument.project_id == id).order_by(SADocument.id).limit(20))

    async def relation(self, tenant, id):
        entity = SAProject if self.orm else SAProject.__table__
        project = await self.read(select(entity).where(SAProject.tenant == tenant, SAProject.id == id), True)
        return None if project is None else (project, await self.children(tenant, id))

    async def native_relation(self, tenant, id):
        if not self.orm:
            raise NotImplementedError()
        async with self.sessions() as session:
            project = (await session.execute(select(SAProject).where(SAProject.tenant == tenant, SAProject.id == id).options(selectinload(SAProject.documents)))).scalar_one_or_none()
            return None if project is None else (project, list(project.documents))

    async def create(self, row):
        if self.orm:
            async with self.sessions.begin() as session:
                session.add(SADocument(**row))
            return 1
        async with self.engine.begin() as conn:
            return (await conn.execute(insert(SADocument.__table__).values(**row))).rowcount

    async def remove(self, tenant, id):
        return await self.mutation(delete(SADocument).where(SADocument.tenant == tenant, SADocument.id == id))

    async def cas(self, tenant, id, expected, note):
        return await self.mutation(update(SADocument).where(SADocument.tenant == tenant, SADocument.id == id, SADocument.version == expected).values(version=SADocument.version+1, note=note))

    async def mutation(self, statement):
        if self.orm:
            async with self.sessions.begin() as session:
                return (await session.execute(statement)).rowcount
        async with self.engine.begin() as conn:
            return (await conn.execute(statement)).rowcount

    async def rolled_back_create(self, row):
        if self.orm:
            async with self.sessions.begin() as session:
                session.add(SADocument(**row))
                await session.flush()
                raise RollbackProbe()
        else:
            async with self.engine.begin() as conn:
                await conn.execute(insert(SADocument.__table__).values(**row))
                raise RollbackProbe()

    async def close(self):
        await self.engine.dispose()


class ORM(SQLAlchemy):
    orm = True
    name = "sqlalchemy-orm"


class RollbackProbe(Exception):
    pass


PROVIDERS = [Native, ORM, SQLAlchemy, Raw]
