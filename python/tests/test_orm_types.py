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

from neutron.orm import EndpointIdentity

def static_endpoint_evidence(db: Database) -> None:
    assert_type(db.endpoint_identity,EndpointIdentity|None)

from neutron.orm import Association, LoadBudget, Relation, async_load_many, load_many

def static_relation_consumer(db: Database,rel: Relation[User,User],parents: list[User]) -> None:
    assert_type(load_many(db,rel,parents,budget=LoadBudget(10,20,5)),tuple[Association[User,User],...])

async def static_async_relation_consumer(db: AsyncDatabase,rel: Relation[User,User],parents: list[User]) -> None:
    assert_type(await async_load_many(db,rel,parents,budget=LoadBudget(10,20,5)),tuple[Association[User,User],...])


def static_refresh_consumer(session: Session,obj: User) -> None:
    assert_type(session.refresh(obj),User)
    assert_type(session.detach(obj),None)

async def static_async_refresh_consumer(session: AsyncSession,obj: User) -> None:
    assert_type(await session.refresh(obj),User)
    assert_type(session.detach(obj),None)


def static_attach_existing_consumer(session: Session,mapping: ModelMapping[User],obj: User) -> None:
    assert_type(session.attach_existing(mapping,obj),User)

async def static_async_attach_existing_consumer(session: AsyncSession,mapping: ModelMapping[User],obj: User) -> None:
    assert_type(await session.attach_existing(mapping,obj,discard_changes=True),User)

from typing import Iterator,AsyncIterator
from neutron.orm import Stream,AsyncStream

def static_stream_consumer(db: Database) -> None:
    t=Table('t',{'id':ColumnSpec(int,'int4')})
    assert_type(db.stream(select(t.column('id',int)),batch_size=10),Stream[int])
    with db.stream(select(t.column('id',int)),batch_size=10) as rows:
        assert_type(rows,Iterator[int]);assert_type(next(rows),int)

async def static_async_stream_consumer(db: AsyncDatabase) -> None:
    t=Table('t',{'id':ColumnSpec(int,'int4')})
    assert_type(db.stream(select(t.column('id',int)),batch_size=10),AsyncStream[int])
    async with db.stream(select(t.column('id',int)),batch_size=10) as rows:
        assert_type(rows,AsyncIterator[int]);assert_type(await anext(rows),int)
