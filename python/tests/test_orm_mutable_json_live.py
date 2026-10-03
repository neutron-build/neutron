from dataclasses import dataclass
from decimal import Decimal
import pytest
from neutron.orm import AsyncSession,ColumnSpec,ModelMapping,MutableJson,OrmError,Session,Table
from .test_orm_live import live_table

@dataclass
class Document:
    id: int
    body: MutableJson|None

@pytest.fixture
def mutable_json(live_table):
    url,base,native=live_table
    t=Table('mutable_json',{'id':ColumnSpec(int,'int4'),'body':ColumnSpec(MutableJson,'jsonb',nullable=True)},schema=base.schema)
    native.execute(f'CREATE TABLE {t.sql}(id int PRIMARY KEY,body jsonb)')
    m=ModelMapping(Document,t,dict(t.columns),primary_key=('id',),instrumented=True)
    return url,m,native


def test_native_nested_mutation_exact_numbers_rollback_and_nulls(mutable_json):
    url,m,native=mutable_json
    with Session.connect(url) as session:
        obj=Document(1,MutableJson({'items':[{'value':Decimal('0.1000000000000000000001')}],'enabled':False}))
        sql_null=Document(2,None);json_null=Document(3,MutableJson(None))
        session.add(m,obj);session.add(m,sql_null);session.add(m,json_null);session.commit()
        assert native.execute(f'SELECT id,body IS NULL,jsonb_typeof(body) FROM {m.table.sql} ORDER BY id').fetchall()==[(1,False,'object'),(2,True,None),(3,False,'null')]
        obj.body.value['items'][0]['value']=Decimal('0.2000000000000000000002')
        obj.body.value['items'].append({'value':None})
        obj.body.value['enabled']=True
        session.flush();session.rollback()
        assert obj.body.value=={'items':[{'value':Decimal('0.1000000000000000000001')}],'enabled':False}
        obj.body.value['items'].append({'value':Decimal('0.3000000000000000000003')})
        session.commit()
        assert native.execute(f"SELECT body->'items'->1->>'value' FROM {m.table.sql} WHERE id=1").fetchone()==('0.3000000000000000000003',)
        session.refresh(obj);session.commit()
        # Refresh must not alias the object tree with its persisted snapshot.
        obj.body.value['items'].append({'value':4});session.commit()
        assert native.execute(f"SELECT jsonb_array_length(body->'items') FROM {m.table.sql} WHERE id=1").fetchone()==(3,)
        obj.body.value['bad']=float('nan')
        with pytest.raises(ValueError): session.flush()
        session.rollback();assert 'bad' not in obj.body.value


def test_native_mutable_savepoint_and_hook_changes_restore_deep_baseline(mutable_json):
    url,m,native=mutable_json
    with Session.connect(url) as session:
        obj=Document(1,MutableJson({'values':[1]}));session.add(m,obj);session.commit()
        with pytest.raises(RuntimeError):
            with session.savepoint():
                obj.body.value['values'].append(2);session.flush()
                raise RuntimeError('restore deep tree')
        assert obj.body.value=={'values':[1]}
        def modify(event): event.obj.body.value['values'].append(3)
        session.listen('before_update',modify)
        obj.body.value['values'].append(2);session.commit()
        assert obj.body.value=={'values':[1,2,3]}
        assert native.execute(f"SELECT body->'values' FROM {m.table.sql}").fetchone()==([1,2,3],)

@pytest.mark.asyncio
async def test_native_async_mutable_nested_tracking(mutable_json):
    url,m,native=mutable_json
    async with await AsyncSession.connect(url) as session:
        obj=Document(1,MutableJson({'nested':{'items':[]}}));session.add(m,obj);await session.commit()
        obj.body.value['nested']['items'].append(Decimal('0.123456789012345678901'))
        await session.commit()
        assert native.execute(f"SELECT body->'nested'->'items'->>0 FROM {m.table.sql}").fetchone()==('0.123456789012345678901',)
