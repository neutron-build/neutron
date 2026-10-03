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

from neutron.orm import SessionEvent

def static_event_consumer(session: Session) -> None:
    def observe(event: SessionEvent) -> None:
        assert_type(event.obj,object|None)
    session.listen('before_flush',observe)

async def static_async_event_consumer(session: AsyncSession) -> None:
    async def observe(event: SessionEvent) -> None: pass
    session.listen('before_flush',observe)

from neutron.orm import alias,exists,in_query

def static_correlated_alias_consumer(db: Database) -> None:
    t=Table('t',{'id':ColumnSpec(int,'int4')})
    a=alias(t,'a');b=alias(t,'b');aid=a.column('id',int);bid=b.column('id',int)
    sub=query_from(b).correlate(a).select(field(bid)).where(bid.eq(aid))
    assert_type(db.all(query_from(a).select(field(aid)).where(exists(sub))),list[int])
    assert_type(db.all(query_from(a).select(field(aid)).where(in_query(aid,sub))),list[int])

from decimal import Decimal
from neutron.orm import avg,count,min_value,row_number,sum_value

def static_aggregate_consumer(db: Database) -> None:
    t=Table('t',{'id':ColumnSpec(int,'int4')});col=t.column('id',int)
    assert_type(db.all(query_from(t).select(count(col))),list[int])
    assert_type(db.all(query_from(t).select(sum_value(col))),list[int|Decimal|None])
    assert_type(db.all(query_from(t).select(avg(col))),list[Decimal|None])
    assert_type(db.all(query_from(t).select(min_value(col))),list[int|None])
    assert_type(db.all(query_from(t).select(row_number(t))),list[int])
    q=query_from(t).select(field(col))
    assert_type(db.all(q.union(q)),list[int])

from neutron.orm import cte,derived

def static_derived_consumer(db: Database) -> None:
    t=Table('t',{'id':ColumnSpec(int,'int4')});q=query_from(t).select(field(t.column('id',int)))
    projected=derived(q,'projected',labels=('id',))
    assert_type(db.all(query_from(projected).select(field(projected.column('id',int)))),list[int])
    named=cte(q,'named',labels=('id',))
    assert_type(db.all(query_from(named).select(field(named.column('id',int)))),list[int])

from typing import Sequence

def static_graph_consumer(session: Session,rel: Relation[User,User],parent: User,children: Sequence[User]) -> None:
    assert_type(session.add_graph(rel,parent,children),None)
    assert_type(session.link(rel,parent,parent),None)
    assert_type(session.load_relation(rel,[parent],budget=LoadBudget(1,1,1)),tuple[Association[User,User],...])

async def static_async_graph_consumer(session: AsyncSession,rel: Relation[User,User],parent: User) -> None:
    assert_type(await session.load_relation(rel,[parent],budget=LoadBudget(1,1,1)),tuple[Association[User,User],...])

from neutron.orm import Predicate

def static_bulk_consumer(session: Session,mapping: ModelMapping[User],where: Predicate) -> None:
    assert_type(session.bulk_update(mapping,{'name':'bulk'},where=where),int)
    assert_type(session.bulk_delete(mapping,where=where),int)

async def static_async_bulk_consumer(session: AsyncSession,mapping: ModelMapping[User],where: Predicate) -> None:
    assert_type(await session.bulk_update(mapping,{'name':'bulk'},where=where),int)
    assert_type(await session.bulk_delete(mapping,where=where),int)


def static_merge_consumer(session: Session,mapping: ModelMapping[User],obj: User) -> None:
    assert_type(session.merge(mapping,obj,expected=mapping.snapshot(obj)),User)

async def static_async_merge_consumer(session: AsyncSession,mapping: ModelMapping[User],obj: User) -> None:
    assert_type(await session.merge(mapping,obj,expected=mapping.snapshot(obj)),User)


def static_savepoint_consumer(session: Session) -> None:
    with session.savepoint() as nested: assert_type(nested,Session)

async def static_async_savepoint_consumer(session: AsyncSession) -> None:
    async with session.savepoint() as nested: assert_type(nested,AsyncSession)


def static_expiration_consumer(session: Session,obj: User) -> None:
    assert_type(session.expire(obj,'name'),None)

from neutron.orm import MutableJson

def static_mutable_codec_consumer(db: Database) -> None:
    t=Table('documents',{'body':ColumnSpec(MutableJson,'jsonb',nullable=True)})
    assert_type(db.all(select(t.nullable_column('body',MutableJson))),list[MutableJson|None])
