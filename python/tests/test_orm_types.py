"""Static consumer contract, also safe to collect in ordinary pytest."""
from typing import assert_type
from neutron.orm import AsyncDatabase, ColumnSpec, Database, Table, select


def static_sync_consumer(db: Database) -> None:
    t=Table('t',{'id':ColumnSpec(int,'int4'),'n':ColumnSpec(int,'int4',nullable=True)})
    assert_type(db.all(select(t.column('id',int))),list[int])
    assert_type(db.one(select(t.column('id',int))),int)
    assert_type(db.one_or_none(select(t.column('id',int))),int|None)
    assert_type(db.all(select(t.nullable_column('n',int))),list[int|None])

async def static_async_consumer(db: AsyncDatabase) -> None:
    t=Table('t',{'id':ColumnSpec(int,'int4')})
    assert_type(await db.all(select(t.column('id',int))),list[int])


def static_session_consumer(session: 'Session',mapping: 'ModelMapping[User]') -> None:
    assert_type(session.get(mapping,1),User|None)

async def static_async_session_consumer(session: 'AsyncSession',mapping: 'ModelMapping[User]') -> None:
    assert_type(await session.get(mapping,1),User|None)

from dataclasses import dataclass
from neutron.orm import AsyncSession, ModelMapping, Session
@dataclass
class User:
    id: int
