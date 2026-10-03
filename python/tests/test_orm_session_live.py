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


def test_native_attach_existing_match_discard_cache_and_no_insert(mapped):
    url,m,native=mapped
    row=native.execute(f'INSERT INTO {m.table.sql}(name,active) VALUES (%s,%s) RETURNING id',('stored',False)).fetchone()
    obj=User(row[0],'stored',False)
    with Session.connect(url) as session:
        assert session.attach_existing(m,obj) is obj
        assert session.get(m,obj.id) is obj
        with pytest.raises(OrmError,match='identity'): session.attach_existing(m,User(obj.id,'stored',False))
        session.rollback();session.detach(obj)
    obj.name='unsaved'
    with Session.connect(url) as session:
        with pytest.raises(ConflictError): session.attach_existing(m,obj)
        assert obj.name=='unsaved' and session.object_state(obj) is ObjectState.TRANSIENT
        session.rollback()
        assert session.attach_existing(m,obj,discard_changes=True) is obj
        assert obj.name=='stored'
        obj.name='changed';session.flush();session.rollback()
        assert obj.name=='stored' and session.object_state(obj) is ObjectState.PERSISTENT
        assert native.execute(f'SELECT count(*),min(name) FROM {m.table.sql}').fetchone()==(1,'stored')
        session.detach(obj)
        missing=User(9999,'absent')
        with pytest.raises(ConflictError): session.attach_existing(m,missing)
        assert session.object_state(missing) is ObjectState.TRANSIENT
        session.rollback()

@pytest.mark.asyncio
async def test_native_async_attach_existing_and_cross_session_release(mapped):
    url,m,native=mapped
    ident=native.execute(f'INSERT INTO {m.table.sql}(name,active) VALUES (%s,%s) RETURNING id',('stored',True)).fetchone()[0]
    obj=User(ident,'stored',True)
    async with await AsyncSession.connect(url) as first:
        assert await first.attach_existing(m,obj) is obj
        await first.commit();first.detach(obj)
    obj.name='local'
    async with await AsyncSession.connect(url) as second:
        with pytest.raises(ConflictError): await second.attach_existing(m,obj)
        await second.rollback()
        assert await second.attach_existing(m,obj,discard_changes=True) is obj
        obj.active=False;await second.commit()
        assert native.execute(f'SELECT name,active FROM {m.table.sql} WHERE id=%s',(ident,)).fetchone()==('stored',False)


def test_native_ordered_hooks_prevalidation_and_known_commit(mapped):
    url,m,native=mapped
    with Session.connect(url) as session:
        events=[]
        for name in ('before_flush','before_insert','before_update','before_delete','after_flush','after_commit'):
            session.listen(name,lambda event: events.append((event.name,event.obj)))
        first=User(name='first');second=User(name='second')
        session.add(m,first);session.add(m,second);session.commit()
        assert [name for name,_ in events]==['before_flush','before_insert','before_insert','after_flush','after_commit']
        assert events[1][1] is first and events[2][1] is second
        events.clear();first.name='updated';session.commit()
        assert [name for name,_ in events]==['before_flush','before_update','after_flush','after_commit']
        events.clear();session.delete(second);session.commit()
        assert [name for name,_ in events]==['before_flush','before_delete','after_flush','after_commit']
        assert native.execute(f'SELECT name FROM {m.table.sql}').fetchall()==[('updated',)]
    with Session.connect(url) as session:
        first=User(name='safe');second=User(name='invalid');session.add(m,first);session.add(m,second)
        def invalidate(event):
            if event.obj is second: second.name=123
        session.listen('before_insert',invalidate)
        with pytest.raises(ValueError): session.flush()
        assert first.id is None
        with pytest.raises(OrmError,match='requires rollback'): session.commit()
        session.rollback()
        assert native.execute(f'SELECT count(*) FROM {m.table.sql}').fetchone()==(1,)


def test_native_hook_exception_reentrancy_and_after_commit_outcome(mapped):
    url,m,native=mapped
    with Session.connect(url) as session:
        obj=User(name='refused');session.add(m,obj)
        session.listen('before_insert',lambda event: session.flush())
        with pytest.raises(SessionBusyError,match='reentrancy'): session.flush()
        session.rollback();assert obj.id is None
        assert native.execute(f'SELECT count(*) FROM {m.table.sql}').fetchone()==(0,)
    with Session.connect(url) as session:
        obj=User(name='durable');session.add(m,obj)
        def fail(event): raise RuntimeError('application callback failed')
        session.listen('after_commit',fail)
        with pytest.raises(RuntimeError): session.commit()
        assert session.object_state(obj) is ObjectState.PERSISTENT
        assert native.execute(f'SELECT name FROM {m.table.sql}').fetchone()==('durable',)
        # A postcommit notification failure must never undo the known result.
        session.rollback();assert obj.name=='durable'


@pytest.mark.asyncio
async def test_native_async_hooks_await_order_and_rollback(mapped):
    url,m,native=mapped
    async with await AsyncSession.connect(url) as session:
        events=[]
        async def observe(event):
            await asyncio.sleep(0)
            events.append(event.name)
        for name in ('before_flush','before_insert','after_flush','after_commit','after_rollback'):
            session.listen(name,observe)
        obj=User(name='async_hooks');session.add(m,obj);await session.commit()
        assert events==['before_flush','before_insert','after_flush','after_commit']
        obj.name='changed'
        async def fail(event):
            with pytest.raises(SessionBusyError): await session.commit()
            raise RuntimeError('refuse before commit')
        session.listen('after_flush',fail)
        with pytest.raises(RuntimeError): await session.flush()
        await session.rollback()
        assert obj.name=='async_hooks' and events[-1]=='after_rollback'
        assert native.execute(f'SELECT name FROM {m.table.sql}').fetchone()==('async_hooks',)


def test_native_row_callback_cannot_introduce_unannounced_write(mapped):
    url,m,native=mapped
    with Session.connect(url) as session:
        a=User(name='a');b=User(name='b');session.add(m,a);session.add(m,b);session.commit()
        a.name='change_a'
        def change_other(event): b.name='change_b'
        session.listen('before_update',change_other)
        with pytest.raises(OrmError,match='new write actions'): session.flush()
        assert native.execute(f'SELECT name FROM {m.table.sql} ORDER BY id').fetchall()==[('a',),('b',)]
        session.rollback();assert (a.name,b.name)==('a','b')

@pytest.mark.asyncio
async def test_native_async_row_callback_cannot_introduce_unannounced_write(mapped):
    url,m,native=mapped
    async with await AsyncSession.connect(url) as session:
        a=User(name='a');b=User(name='b');session.add(m,a);session.add(m,b);await session.commit()
        a.name='change_a'
        async def change_other(event): b.name='change_b'
        session.listen('before_update',change_other)
        with pytest.raises(OrmError,match='new write actions'): await session.flush()
        assert native.execute(f'SELECT name FROM {m.table.sql} ORDER BY id').fetchall()==[('a',),('b',)]
        await session.rollback();assert (a.name,b.name)==('a','b')


def test_native_raw_control_refusal_preserves_owned_transaction_rows(mapped):
    from neutron.orm import Mutation
    url,m,native=mapped
    with Database.connect(url) as db:
        with Session(db) as session:
            obj=User(name='uncommitted');session.add(m,obj);session.flush()
            for sql in ('COMMIT','/* hide */ ROLLBACK','SELECT 1; COMMIT','SET LOCAL ROLE postgres'):
                with pytest.raises(OrmError): db.execute(Mutation(sql,()))
            assert native.execute(f'SELECT count(*) FROM {m.table.sql}').fetchone()==(0,)
            db.execute(Mutation(f'UPDATE {m.table.sql} SET name=%s WHERE id=%s',('raw',obj.id)))
            session.rollback();assert obj.id is None
    assert native.execute(f'SELECT count(*) FROM {m.table.sql}').fetchone()==(0,)

@dataclass
class Versioned:
    id: int
    value: str
    version: int=1


def test_native_versioned_flush_increment_rollback_and_stale_conflict(live_table):
    url,base,native=live_table
    t=Table('versioned',{'id':ColumnSpec(int,'int4'),'value':ColumnSpec(str,'text'),'version':ColumnSpec(int,'int4')},schema=base.schema)
    native.execute(f'CREATE TABLE {t.sql}(id int PRIMARY KEY,value text NOT NULL,version int NOT NULL)')
    m=ModelMapping(Versioned,t,dict(t.columns),primary_key=('id',),version_field='version')
    with Session.connect(url) as session:
        obj=Versioned(1,'first');session.add(m,obj);session.commit()
        obj.value='second';session.flush();assert obj.version==2
        session.rollback();assert (obj.value,obj.version)==('first',1)
        obj.value='third';session.commit();assert obj.version==2
        native.execute(f'UPDATE {t.sql} SET value=%s,version=version+1 WHERE id=1',('external',))
        obj.value='stale'
        with pytest.raises(ConflictError): session.flush()
        session.rollback();assert (obj.value,obj.version)==('third',2)
        obj.version=99
        with pytest.raises(OrmError,match='mapped version'): session.flush()
        session.rollback()
        assert native.execute(f'SELECT value,version FROM {t.sql}').fetchone()==('external',3)


def test_native_bulk_synchronizes_tracked_rows_and_rollback(mapped):
    url,m,native=mapped
    with Session.connect(url) as session:
        a=User(name='a');b=User(name='b');session.add(m,a);session.add(m,b);session.commit()
        identity=a.id
        assert session.bulk_update(m,{'active':False},where=m.table.column('id',int).eq(identity))==1
        assert a.active is False and b.active is True
        session.rollback();assert a.active is True
        assert native.execute(f'SELECT active FROM {m.table.sql} WHERE id=%s',(identity,)).fetchone()==(True,)
        assert session.bulk_delete(m,where=m.table.column('id',int).eq(identity))==1
        assert session.object_state(a) is ObjectState.DELETED
        assert session.get(m,identity) is None
        session.rollback();assert session.object_state(a) is ObjectState.PERSISTENT
        assert session.get(m,identity) is a
        with pytest.raises(OrmError,match='primary-key'): session.bulk_update(m,{'id':9},where=m.table.column('id',int).eq(identity))
        assert session.bulk_delete(m,where=m.table.column('id',int).eq(identity))==1
        session.commit();assert session.object_state(a) is ObjectState.DELETED
        assert native.execute(f'SELECT count(*) FROM {m.table.sql}').fetchone()==(1,)

@pytest.mark.asyncio
async def test_native_async_bulk_sync_and_rollback(mapped):
    url,m,native=mapped
    async with await AsyncSession.connect(url) as session:
        obj=User(name='async');session.add(m,obj);await session.commit()
        assert await session.bulk_update(m,{'active':False},where=m.table.column('id',int).eq(obj.id))==1
        assert obj.active is False
        await session.rollback();assert obj.active is True
        assert await session.bulk_delete(m,where=m.table.column('id',int).eq(obj.id))==1
        await session.rollback();assert session.object_state(obj) is ObjectState.PERSISTENT
        assert native.execute(f'SELECT count(*) FROM {m.table.sql}').fetchone()==(1,)


def test_native_detached_merge_explicit_baseline_conflict_and_rollback(mapped):
    url,m,native=mapped
    with Session.connect(url) as session:
        obj=User(name='original');session.add(m,obj);session.commit();expected=m.snapshot(obj);session.detach(obj)
        obj.name='patch'
        target=session.merge(m,obj,expected=expected)
        assert target is not obj and target.name=='patch'
        assert session.get(m,obj.id) is target
        session.rollback();assert target.name=='original' and obj.name=='patch'
        target=session.merge(m,obj,expected=expected);session.commit()
        assert native.execute(f'SELECT name FROM {m.table.sql}').fetchone()==('patch',)
        native.execute(f'UPDATE {m.table.sql} SET name=%s',('external',))
        with pytest.raises(ConflictError): session.merge(m,obj,expected=expected)
        session.rollback();assert target.name=='patch'
        assert native.execute(f'SELECT name FROM {m.table.sql}').fetchone()==('external',)

@pytest.mark.asyncio
async def test_native_async_detached_merge_and_owner_refusal(mapped):
    url,m,native=mapped
    async with await AsyncSession.connect(url) as session:
        obj=User(name='original');session.add(m,obj);await session.commit();expected=m.snapshot(obj)
        with pytest.raises(OrmError,match='detached'): await session.merge(m,obj,expected=expected)
        session.detach(obj);obj.name='async_patch'
        target=await session.merge(m,obj,expected=expected)
        assert target is not obj
        await session.commit()
        assert native.execute(f'SELECT name FROM {m.table.sql}').fetchone()==('async_patch',)


def test_native_savepoint_reconciles_partial_flush_and_outer_commit(mapped):
    url,m,native=mapped
    with Session.connect(url) as session:
        anchor=User(name='anchor');session.add(m,anchor)
        with pytest.raises(OrmError) as failed:
            with session.savepoint():
                first=User(name='duplicate');last=User(name='duplicate')
                session.add(m,first);session.add(m,last);session.flush()
        assert failed.value.sqlstate=='23505'
        assert first.id is None and last.id is None
        assert session.object_state(first) is ObjectState.TRANSIENT
        assert anchor.id is not None
        anchor.name='outer_survives';session.commit()
        assert native.execute(f'SELECT name FROM {m.table.sql}').fetchall()==[('outer_survives',)]
        with session.savepoint():
            with pytest.raises(SessionBusyError): session.commit()
            with pytest.raises(SessionBusyError): session.rollback()
            with pytest.raises(SessionBusyError): session.close()
            with pytest.raises(RuntimeError):
                with session.savepoint():
                    anchor.name='inner';session.flush();raise RuntimeError('rollback inner')
            assert anchor.name=='outer_survives'
            anchor.name='saved';session.flush()
        session.rollback();assert anchor.name=='outer_survives'
        assert native.execute(f'SELECT name FROM {m.table.sql}').fetchall()==[('outer_survives',)]

@pytest.mark.asyncio
async def test_native_async_savepoint_rollback_checkpoint_and_reuse(mapped):
    url,m,native=mapped
    async with await AsyncSession.connect(url) as session:
        anchor=User(name='anchor');session.add(m,anchor);await session.commit()
        with pytest.raises(RuntimeError):
            async with session.savepoint():
                anchor.name='inner';await session.flush()
                extra=User(name='extra');session.add(m,extra);await session.flush()
                with pytest.raises(SessionBusyError): await session.commit()
                raise RuntimeError('rollback savepoint')
        assert anchor.name=='anchor' and extra.id is None
        assert session.object_state(extra) is ObjectState.TRANSIENT
        anchor.name='survives';await session.commit()
        assert native.execute(f'SELECT name FROM {m.table.sql}').fetchall()==[('survives',)]


def test_native_savepoint_exit_flushes_constraints_inside_boundary(mapped):
    url,m,native=mapped
    with Session.connect(url) as session:
        with pytest.raises(OrmError) as duplicate:
            with session.savepoint():
                a=User(name='duplicate');b=User(name='duplicate')
                session.add(m,a);session.add(m,b)
        assert duplicate.value.sqlstate=='23505' and a.id is None and b.id is None
        good=User(name='survives');session.add(m,good);session.commit()
        assert native.execute(f'SELECT name FROM {m.table.sql}').fetchall()==[('survives',)]
