from dataclasses import dataclass
import pytest
from neutron.orm import ColumnSpec, Database, ModelMapping, ObjectState, OrmError, Session, Table
from .test_orm_clients import Connection

@dataclass
class User:
    id: int | None=None
    name: str='new'


def mapping():
    t=Table('t',{'id':ColumnSpec(int,'int4',generated=True),'name':ColumnSpec(str,'text')})
    return ModelMapping(User,t,{'id':t.column('id',int),'name':t.column('name',str)},primary_key=('id',))


def test_real_session_object_ownership_releases_without_io():
    mapper=mapping();user=User();one=Session(Database(Connection([])));two=Session(Database(Connection([])))
    one.add(mapper,user)
    with pytest.raises(OrmError,match='another Session'): two.add(mapper,user)
    one.rollback();assert one.object_state(user) is ObjectState.TRANSIENT
    two.add(mapper,user);two.close();one.close()


def test_fenced_database_blocks_identity_reads_and_mapping_conflicts():
    mapper=mapping();db=Database(Connection([]));session=Session(db)
    session.add(mapper,User())
    with pytest.raises(OrmError,match='different metadata'): session.add(mapping(),User())
    db.close()
    with pytest.raises(OrmError,match='fenced'): session.get(mapper,1)
    session.close()


def test_detach_conservative_and_refresh_preconditions_without_io():
    mapper=mapping();session=Session(Database(Connection([])));obj=User(1,'original')
    record=session._store.attach(mapper,obj,new=False)
    obj.name='dirty'
    with pytest.raises(OrmError,match='dirty'): session.detach(obj)
    with pytest.raises(OrmError,match='dirty'): session._refresh_record(obj,False)
    assert session._refresh_record(obj,True) is record
    obj.name='original';session._transaction=object()
    with pytest.raises(OrmError,match='active transaction'): session.detach(obj)
    session._transaction=None;session.detach(obj)
    assert session.object_state(obj) is ObjectState.DETACHED
    with pytest.raises(OrmError): session._refresh_record(obj,True)
    session.close()


def test_refresh_refuses_pending_flushed_new_deleted_and_pk_mutation():
    mapper=mapping();session=Session(Database(Connection([])));obj=User()
    session.add(mapper,obj)
    with pytest.raises(OrmError): session._refresh_record(obj,True)
    record=session._store.records[id(obj)]
    session._store.flushed(record,{'id':1,'name':'new'})
    with pytest.raises(OrmError): session._refresh_record(obj,True)
    session._store.committed()
    session.delete(obj)
    with pytest.raises(OrmError): session._refresh_record(obj,True)
    session.rollback();obj.id=2
    with pytest.raises(OrmError,match='primary-key'): session._refresh_record(obj,True)
    session.rollback();session.close()

@pytest.mark.asyncio
async def test_refresh_cancellation_fences_async_session_until_rollback(monkeypatch):
    import asyncio
    from neutron.orm import AsyncDatabase,AsyncSession
    from .test_orm_clients import AsyncConnection
    mapper=mapping();obj=User(1,'original');db=AsyncDatabase(AsyncConnection([]));session=AsyncSession(db)
    session._store.attach(mapper,obj,new=False)
    async def cancelled(query): raise asyncio.CancelledError()
    monkeypatch.setattr(db,'one',cancelled)
    with pytest.raises(asyncio.CancelledError): await session.refresh(obj)
    assert obj.name=='original'
    with pytest.raises(OrmError,match='requires rollback'): await session.get(mapper,1)
    await session.rollback();session.detach(obj)
    assert session.object_state(obj) is ObjectState.DETACHED
    await session.close()
