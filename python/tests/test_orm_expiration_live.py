from dataclasses import dataclass
import pytest
from neutron.orm import AsyncSession,ColumnSpec,ExpiredAttributeError,ModelMapping,ObjectState,OrmError,Session,Table
from .test_orm_live import live_table

@dataclass
class Instrumented:
    id: int
    name: str
    active: bool=True

@pytest.fixture
def expiration(live_table):
    url,base,native=live_table
    t=Table('instrumented',{'id':ColumnSpec(int,'int4'),'name':ColumnSpec(str,'text'),'active':ColumnSpec(bool,'bool')},schema=base.schema)
    native.execute(f'CREATE TABLE {t.sql}(id int PRIMARY KEY,name text NOT NULL,active bool NOT NULL)')
    native.execute(f"INSERT INTO {t.sql} VALUES (1,'original',true)")
    mapping=ModelMapping(Instrumented,t,dict(t.columns),primary_key=('id',),instrumented=True)
    return url,mapping,native


def test_native_expired_properties_refuse_io_and_refresh_same_identity(expiration):
    url,m,native=expiration
    with Session.connect(url) as session:
        obj=session.get(m,1);session.commit();session.expire(obj,'name')
        assert session.object_state(obj) is ObjectState.EXPIRED and obj.id==1
        with pytest.raises(ExpiredAttributeError): _=obj.name
        with pytest.raises(ExpiredAttributeError): obj.name='blind_edit'
        native.execute(f'UPDATE {m.table.sql} SET name=%s',('external',))
        assert session.refresh(obj) is obj and obj.name=='external'
        session.rollback();assert obj.name=='original'
        obj.name='dirty'
        with pytest.raises(OrmError,match='dirty'): session.expire(obj)
        session.expire(obj,discard_changes=True)
        with pytest.raises(ExpiredAttributeError): _=obj.id
        assert session.get(m,1) is obj and obj.name=='external'
        session.commit();session.detach(obj)
        assert session.object_state(obj) is ObjectState.DETACHED and obj.name=='external'


def test_native_expiration_savepoint_flags_and_rollback(expiration):
    url,m,native=expiration
    with Session.connect(url) as session:
        obj=session.get(m,1);session.commit();session.expire(obj,'name')
        with pytest.raises(RuntimeError):
            with session.savepoint():
                session.refresh(obj);obj.name='inner';session.flush()
                raise RuntimeError('restore expiration flag')
        assert session.object_state(obj) is ObjectState.EXPIRED
        with pytest.raises(ExpiredAttributeError): _=obj.name
        session.rollback();assert obj.name=='original'
        assert native.execute(f'SELECT name FROM {m.table.sql}').fetchone()==('original',)

@pytest.mark.asyncio
async def test_native_async_expire_on_commit_and_explicit_await_refresh(expiration):
    url,m,native=expiration
    async with await AsyncSession.connect(url,expire_on_commit=True) as session:
        obj=await session.get(m,1);await session.commit()
        assert session.object_state(obj) is ObjectState.EXPIRED
        with pytest.raises(ExpiredAttributeError): _=obj.name
        native.execute(f'UPDATE {m.table.sql} SET name=%s',('external',))
        assert await session.get(m,1) is obj and obj.name=='external'
        obj.name='updated';await session.commit()
        with pytest.raises(ExpiredAttributeError): _=obj.name
        assert native.execute(f'SELECT name FROM {m.table.sql}').fetchone()==('updated',)
        await session.refresh(obj);assert obj.name=='updated'
        await session.rollback()


def test_native_closed_expired_object_requires_explicit_reattachment(expiration):
    url,m,native=expiration
    with Session.connect(url) as session:
        obj=session.get(m,1);session.commit();session.expire(obj,'name')
    with pytest.raises(ExpiredAttributeError): _=obj.name
    assert obj.id==1
    native.execute(f'UPDATE {m.table.sql} SET name=%s',('fresh',))
    with Session.connect(url) as other:
        assert other.attach_existing(m,obj,discard_changes=True) is obj
        assert obj.name=='fresh'
