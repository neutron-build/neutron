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

from neutron.orm import Order, field, outer_field, query_from

def static_join_consumer(db: Database) -> None:
    a=Table('a',{'id':ColumnSpec(int,'int4')})
    b=Table('b',{'owner':ColumnSpec(int,'int4'),'label':ColumnSpec(str,'text')})
    scope=query_from(a).left_join(b,on=a.column('id',int).eq(b.column('owner',int)))
    result=db.all(scope.select_pair(field(a.column('id',int)),outer_field(b.column('label',str))))
    assert_type(result,list[tuple[int,str|None]])
    assert_type(db.all(scope.select(outer_field(b.column('label',str)))),list[str|None])

def static_join_negative_contract(db: Database) -> None:
    a=Table('a',{'id':ColumnSpec(int,'int4')})
    b=Table('b',{'id':ColumnSpec(int,'int4')})
    scope=query_from(a).left_join(b,on=a.column('id',int).eq(b.column('id',int)))
    nullable_result = db.all(scope.select(outer_field(b.column('id',int))))
    nonnullable: list[int] = nullable_result  # type: ignore[assignment]
    a.column('id',int).eq('wrong')  # type: ignore[arg-type]

from neutron.orm import JsonDocument

def static_json_consumer(db: Database) -> None:
    t=Table('documents',{'value':ColumnSpec(JsonDocument,'jsonb',nullable=True)})
    assert_type(db.all(select(t.nullable_column('value',JsonDocument))),list[JsonDocument|None])
