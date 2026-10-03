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

from neutron.orm import ManyToMany, OwnedRelation

def static_owned_graph_consumer(session: Session,root: OwnedRelation[User,User],descendant: OwnedRelation[User,User],parent: User,through: User,meta: ManyToMany[User,User,User]) -> None:
    assert_type(session.delete_graph(root,parent,budget=LoadBudget(3,10,10),descendants=(descendant,),max_depth=2),None)
    assert_type(session.connect_many_to_many(meta,parent,parent,through),None)
    assert_type(session.disconnect_many_to_many(meta,parent,parent,through),None)

async def static_async_owned_graph_consumer(session: AsyncSession,root: OwnedRelation[User,User],descendant: OwnedRelation[User,User],parent: User,through: User,meta: ManyToMany[User,User,User]) -> None:
    assert_type(await session.delete_graph(root,parent,budget=LoadBudget(3,10,10),descendants=(descendant,),max_depth=2),None)
    assert_type(session.connect_many_to_many(meta,parent,parent,through),None)
    assert_type(session.disconnect_many_to_many(meta,parent,parent,through),None)

from neutron.orm import PgArray, TimeOfDay, Interval, array_spec

def static_component_value_consumer(db: Database) -> None:
    spec=array_spec(int,'int4',nullable=True)
    assert_type(spec,ColumnSpec[PgArray[int]])
    table=Table('components',{'data':spec,'clock':ColumnSpec(TimeOfDay,'time'),'span':ColumnSpec(Interval,'interval')})
    assert_type(db.all(select(table.nullable_column('data',PgArray[int]))),list[PgArray[int]|None])
    assert_type(db.one(select(table.column('clock',TimeOfDay))),TimeOfDay)
    assert_type(db.one(select(table.column('span',Interval))),Interval)

async def static_async_component_value_consumer(db: AsyncDatabase) -> None:
    table=Table('components',{'data':array_spec(int,'int4')})
    assert_type(await db.all(select(table.column('data',PgArray[int]))),list[PgArray[int]])

from neutron.orm import PgRange,range_spec

def static_range_value_consumer(db: Database) -> None:
    spec=range_spec(int,'int4range',nullable=True)
    assert_type(spec,ColumnSpec[PgRange[int]])
    table=Table('range_values',{'value':spec})
    assert_type(db.all(select(table.nullable_column('value',PgRange[int]))),list[PgRange[int]|None])

from neutron.orm import PgDomain,PgEnum
from decimal import Decimal

def static_catalog_value_consumer(db: Database) -> None:
    state=db.enum_spec('app','state')
    amount=db.domain_spec('app','amount',ColumnSpec(Decimal,'numeric'))
    assert_type(state,ColumnSpec[PgEnum])
    assert_type(amount,ColumnSpec[PgDomain[Decimal]])
    table=db.catalog_table('values',{'state':state,'amount':amount},schema='app')
    assert_type(db.one(select(table.column('state',PgEnum))),PgEnum)
    assert_type(db.one(select(table.column('amount',PgDomain[Decimal]))),PgDomain[Decimal])

async def static_async_catalog_value_consumer(db: AsyncDatabase) -> None:
    amount=await db.domain_spec('app','amount',ColumnSpec(Decimal,'numeric'))
    assert_type(amount,ColumnSpec[PgDomain[Decimal]])
    table=await db.catalog_table('values',{'amount':amount},schema='app')
    assert_type(await db.one(select(table.column('amount',PgDomain[Decimal]))),PgDomain[Decimal])

from uuid import UUID

def static_domain_identity_consumer(db: Database) -> None:
    key=db.domain_spec('app','identity',ColumnSpec(UUID,'uuid'))
    enum=db.enum_spec('app','state');domain=db.domain_spec('app','state_domain',enum)
    assert_type(key,ColumnSpec[PgDomain[UUID]])
    assert_type(domain,ColumnSpec[PgDomain[PgEnum]])

from neutron.orm import PolymorphicMapping,PolymorphicView

@dataclass
class SpecialUser(User):
    detail: str='special'

def static_polymorphic_consumer(session: Session,mapping: PolymorphicMapping[User]) -> None:
    view=mapping.subtype(SpecialUser)
    assert_type(view,PolymorphicView[SpecialUser])
    assert_type(session.get(mapping,1),User|None)
    assert_type(session.get(view,1),SpecialUser|None)
    assert_type(session.select_polymorphic(view,max_rows=10),tuple[SpecialUser,...])
    assert_type(session.select_polymorphic(mapping,max_rows=10),tuple[User,...])

async def static_async_polymorphic_consumer(session: AsyncSession,mapping: PolymorphicMapping[User]) -> None:
    assert_type(await session.get(mapping.subtype(SpecialUser),1),SpecialUser|None)
    assert_type(await session.select_polymorphic(mapping.subtype(SpecialUser),max_rows=10),tuple[SpecialUser,...])

from neutron.orm import Inet,CIDR

def static_network_consumer(db: Database) -> None:
    table=Table('networks',{'host':ColumnSpec(Inet,'inet'),'network':ColumnSpec(CIDR,'cidr',nullable=True)})
    assert_type(db.one(select(table.column('host',Inet))),Inet)
    assert_type(db.one(select(table.nullable_column('network',CIDR))),CIDR|None)

async def static_async_network_consumer(db: AsyncDatabase) -> None:
    table=Table('networks',{'host':ColumnSpec(Inet,'inet')})
    assert_type(await db.one(select(table.column('host',Inet))),Inet)

from neutron.orm import PgComposite

def static_composite_consumer(db: Database) -> None:
    spec=db.composite_spec('app','pair',{'id':ColumnSpec(int,'int8',nullable=True)},nullable=True)
    assert_type(spec,ColumnSpec[PgComposite])
    table=db.catalog_table('records',{'value':spec},schema='app')
    assert_type(db.one(select(table.nullable_column('value',PgComposite))),PgComposite|None)

async def static_async_composite_consumer(db: AsyncDatabase) -> None:
    spec=await db.composite_spec('app','pair',{'id':ColumnSpec(int,'int8',nullable=True)})
    assert_type(spec,ColumnSpec[PgComposite])

from neutron.orm import PgVector

def static_vector_consumer(db: Database) -> None:
    spec=db.vector_spec('extensions')
    assert_type(spec,ColumnSpec[PgVector])
    table=db.catalog_table('vectors',{'value':spec},schema='app')
    assert_type(db.one(select(table.column('value',PgVector))),PgVector)

async def static_async_vector_consumer(db: AsyncDatabase) -> None:
    assert_type(await db.vector_spec('extensions'),ColumnSpec[PgVector])

from neutron.orm import QueryEvent,QueryMetrics,QueryObserver

def static_observer_consumer() -> None:
    observer=QueryObserver(capacity=256)
    assert_type(observer.drain(),tuple[QueryEvent,...])
    assert_type(observer.metrics,QueryMetrics)
