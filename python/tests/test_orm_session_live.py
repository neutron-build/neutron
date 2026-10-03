from dataclasses import dataclass
import asyncio
import pytest
from neutron.orm import AsyncDatabase, AsyncSession, ColumnSpec, ConflictError, Database, ModelMapping, ObjectState, OrmError, Session, SessionBusyError, Table
from .test_orm_live import live_table

@dataclass
class User:
    id: int | None=None
    name: str='new'
    active: bool=True

@pytest.fixture
def mapped(live_table):
    url,parent,native=live_table
    native.execute(f'CREATE TABLE "{parent.schema}".mapped_users(id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY, name text NOT NULL UNIQUE, active bool NOT NULL)')
    t=Table('mapped_users',{'id':ColumnSpec(int,'int4',generated=True),'name':ColumnSpec(str,'text'),'active':ColumnSpec(bool,'bool')},schema=parent.schema)
    m=ModelMapping(User,t,{'id':t.column('id',int),'name':t.column('name',str),'active':t.column('active',bool)},primary_key=('id',))
    return url,m,native


def test_scalar_session_flush_identity_rollback_and_commit(mapped):
    url,m,native=mapped
    with Database.connect(url) as db:
        with Session(db) as session:
            obj=User(name='first',active=False);session.add(m,obj)
            assert session.object_state(obj) is ObjectState.PENDING
            session.flush();assert obj.id is not None
            assert native.execute(f'SELECT count(*) FROM {m.table.sql}').fetchone()==(0,)
            session.rollback();assert obj.id is None and session.object_state(obj) is ObjectState.TRANSIENT
            session.add(m,obj);session.commit();identity=obj.id
            assert native.execute(f'SELECT name,active FROM {m.table.sql} WHERE id=%s',(identity,)).fetchone()==('first',False)
            assert session.get(m,identity) is obj
            obj.name='changed';session.commit()
            assert native.execute(f'SELECT name FROM {m.table.sql} WHERE id=%s',(identity,)).fetchone()==('changed',)
            session.delete(obj);session.flush();session.rollback()
            assert session.object_state(obj) is ObjectState.PERSISTENT
            assert native.execute(f'SELECT count(*) FROM {m.table.sql}').fetchone()==(1,)
        assert session.object_state(obj) is ObjectState.DETACHED


def test_failed_flush_requires_rollback_and_all_new_objects_reconcile(mapped):
    url,m,native=mapped
    with Session.connect(url) as session:
        first=User(name='same');second=User(name='same')
        session.add(m,first);session.add(m,second)
        with pytest.raises(OrmError) as exc: session.flush()
        assert exc.value.sqlstate=='23505'
        with pytest.raises(OrmError,match='requires rollback'): session.get(m,1)
        session.rollback();assert first.id is None and second.id is None
        assert native.execute(f'SELECT count(*) FROM {m.table.sql}').fetchone()==(0,)


def test_prevalidation_and_stale_snapshot_conflict(mapped):
    url,m,native=mapped
    with Session.connect(url) as session:
        first=User(name='first');second=User(name='second')
        session.add(m,first);session.add(m,second);second.name=123
        with pytest.raises(ValueError): session.flush()
        assert first.id is None
        assert native.execute(f'SELECT count(*) FROM {m.table.sql}').fetchone()==(0,)
        session.rollback()
        session.add(m,first);session.commit()
        native.execute(f'UPDATE {m.table.sql} SET name=\'external\' WHERE id=%s',(first.id,))
        first.name='mine'
        with pytest.raises(ConflictError): session.flush()
        session.rollback()
        assert native.execute(f'SELECT name FROM {m.table.sql} WHERE id=%s',(first.id,)).fetchone()==('external',)


def test_session_close_never_implicitly_commits_and_explicit_begin(mapped):
    url,m,native=mapped
    with Session.connect(url) as session:
        obj=User(name='never');session.add(m,obj);session.flush()
    assert native.execute(f'SELECT count(*) FROM {m.table.sql}').fetchone()==(0,)
    assert obj.id is None
    with Session.connect(url,autobegin=False) as session:
        obj=User(name='explicit')
        with session.begin(): session.add(m,obj)
    assert native.execute(f'SELECT name FROM {m.table.sql}').fetchone()==('explicit',)

@pytest.mark.asyncio
async def test_async_scalar_session_native_state_and_owner(mapped):
    url,m,native=mapped
    async with await AsyncSession.connect(url) as session:
        obj=User(name='async',active=False);session.add(m,obj);await session.commit()
        assert await session.get(m,obj.id) is obj
        obj.name='updated';await session.commit()
        assert native.execute(f'SELECT name,active FROM {m.table.sql}').fetchone()==('updated',False)
        async def foreign(): return await session.get(m,obj.id)
        with pytest.raises(SessionBusyError): await asyncio.create_task(foreign())
        session.delete(obj);await session.flush();await session.rollback()
        assert native.execute(f'SELECT count(*) FROM {m.table.sql}').fetchone()==(1,)


def test_native_refresh_discard_rollback_and_detach_same_identity(mapped):
    url,m,native=mapped
    with Session.connect(url) as session:
        obj=User(name='original');session.add(m,obj);session.commit()
        native.execute(f'UPDATE {m.table.sql} SET name=%s,active=false WHERE id=%s',('external',obj.id))
        obj.name='local'
        with pytest.raises(OrmError,match='dirty'): session.refresh(obj)
        assert session.refresh(obj,discard_changes=True) is obj
        assert (obj.name,obj.active)==('external',False)
        with pytest.raises(OrmError,match='active transaction'): session.detach(obj)
        session.rollback();assert (obj.name,obj.active)==('original',True)
        session.refresh(obj);session.commit()
        identity=obj.id;session.detach(obj)
        assert session.object_state(obj) is ObjectState.DETACHED
        loaded=session.get(m,identity)
        assert loaded is not None and loaded is not obj and loaded.name=='external'
        session.rollback()
        # add remains INSERT rather than attaching an existing row.
        with pytest.raises(ValueError): session.add(m,obj)


def test_native_refresh_missing_row_fences_until_rollback(mapped):
    url,m,native=mapped
    with Session.connect(url) as session:
        obj=User(name='vanished');session.add(m,obj);session.commit()
        native.execute(f'DELETE FROM {m.table.sql} WHERE id=%s',(obj.id,))
        with pytest.raises(ConflictError): session.refresh(obj)
        with pytest.raises(OrmError,match='requires rollback'): session.get(m,obj.id)
        session.rollback();assert obj.name=='vanished'

@pytest.mark.asyncio
async def test_native_async_refresh_and_detach(mapped):
    url,m,native=mapped
    async with await AsyncSession.connect(url) as session:
        obj=User(name='original');session.add(m,obj);await session.commit()
        native.execute(f'UPDATE {m.table.sql} SET name=%s WHERE id=%s',('external',obj.id))
        obj.name='local'
        with pytest.raises(OrmError,match='dirty'): await session.refresh(obj)
        assert await session.refresh(obj,discard_changes=True) is obj
        await session.rollback();assert obj.name=='original'
        await session.refresh(obj);await session.commit()
        session.detach(obj)
        assert session.object_state(obj) is ObjectState.DETACHED
        loaded=await session.get(m,obj.id)
        assert loaded is not None and loaded is not obj and loaded.name=='external'
        native.execute(f'DELETE FROM {m.table.sql} WHERE id=%s',(obj.id,))
        with pytest.raises(ConflictError): await session.refresh(loaded)
        with pytest.raises(OrmError,match='requires rollback'): await session.get(m,obj.id)
        await session.rollback();assert loaded.name=='external'
