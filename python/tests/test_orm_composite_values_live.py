from dataclasses import dataclass
from decimal import Decimal
import pytest
from neutron.orm import ArrayDimension,AsyncDatabase,AsyncSession,ColumnSpec,Database,ModelMapping,Mutation,PgArray,PgComposite,Session,Table,TimeOfDay,array_spec,insert,select,select_row
from .test_orm_live import live_table

@dataclass
class CompositeRow:
    id: int
    value: PgComposite|None

def components():
    return {'n':ColumnSpec(int,'int8',nullable=True),'amount':ColumnSpec(Decimal,'numeric',nullable=True),'label':ColumnSpec(str,'text',nullable=True),'blob':ColumnSpec(bytes,'bytea',nullable=True),'ints':array_spec(int,'int4',nullable=True),'clock':ColumnSpec(TimeOfDay,'time',nullable=True)}

@pytest.fixture
def composites(live_table):
    url,base,native=live_table;s=base.schema
    native.execute(f'CREATE TYPE "{s}".record_kind AS (n bigint,amount numeric,label text,blob bytea,ints int4[],clock time)')
    native.execute(f'CREATE TABLE "{s}".composites(id int PRIMARY KEY,value "{s}".record_kind)')
    native.execute(f'CREATE TYPE "{s}".unknown_record AS (x point)')
    return url,s,native


def test_native_composite_binary_precision_nulls_components_and_owner(composites):
    url,s,native=composites
    with Database.connect(url) as db:
        spec=db.composite_spec(s,'record_kind',components(),nullable=True);assert spec.native_type is not None
        table=db.catalog_table('composites',{'id':ColumnSpec(int,'int4'),'value':spec},schema=s)
        exact=PgComposite((2**63-1,Decimal('9007199254740993.12345678901234567890'),'"\\,()\n',b'\x00\xff',PgArray((ArrayDimension(2,-3),),(None,7)),TimeOfDay(86_400_000_000)),spec.native_type)
        row=db.one(insert(table,{'id':1,'value':exact}).returning_row());assert row['value']==exact
        db.execute(Mutation("SELECT pg_catalog.set_config('bytea_output','escape',false)",()))
        assert db.one(select(table.nullable_column('value',PgComposite)).where(table.column('id',int).eq(1)))==exact
        all_null=PgComposite((None,)*6,spec.native_type)
        for identifier,value in ((2,None),(3,all_null)):
            db.execute(insert(table,{'id':identifier,'value':value}))
            assert db.one(select(table.nullable_column('value',PgComposite)).where(table.column('id',int).eq(identifier)))==value
        assert db.composite_spec(s,'record_kind',components()).native_type is spec.native_type
        with pytest.raises(ValueError): db.composite_spec(s,'record_kind',{'n':ColumnSpec(str,'text')})
        with pytest.raises(ValueError): db.composite_spec(s,'unknown_record',{'x':ColumnSpec(str,'text',nullable=True)})
        with pytest.raises(ValueError): Table('bad',{'value':spec})
        with Database.connect(url) as other:
            with pytest.raises(ValueError,match='another connection'): other.one(select_row(table))
        mapping=ModelMapping(CompositeRow,table,dict(table.columns),primary_key=('id',))
        with Session(db) as session:
            obj=session.get(mapping,1);assert obj is not None
            obj.value=all_null;session.flush();session.rollback();assert obj.value==exact
    assert native.execute(f'SELECT (value).n,(value).amount::text,(value).label,encode((value).blob,\'hex\'),array_dims((value).ints),(value).ints[-3] IS NULL,(value).clock::text FROM "{s}".composites WHERE id=1').fetchone()==(2**63-1,'9007199254740993.12345678901234567890','"\\,()\n','00ff','[-3:-2]',True,'24:00:00')
    assert native.execute(f'SELECT id,value IS NULL FROM "{s}".composites WHERE id>1 ORDER BY id').fetchall()==[(2,True),(3,True)] # PostgreSQL row IS NULL includes all-NULL records.
    assert native.execute(f'SELECT value::text FROM "{s}".composites WHERE id=3').fetchone()==('(,,,,,)',)

@pytest.mark.asyncio
async def test_native_async_composite_mapping_and_rollback(composites):
    url,s,native=composites
    async with await AsyncDatabase.connect(url) as db:
        spec=await db.composite_spec(s,'record_kind',components(),nullable=True);assert spec.native_type is not None
        table=await db.catalog_table('composites',{'id':ColumnSpec(int,'int4'),'value':spec},schema=s)
        mapping=ModelMapping(CompositeRow,table,dict(table.columns),primary_key=('id',))
        exact=PgComposite((1,Decimal('0.0000000000000000001'),'',b'',PgArray((),()),TimeOfDay(1)),spec.native_type)
        async with AsyncSession(db) as session:
            obj=CompositeRow(1,exact);session.add(mapping,obj);await session.commit()
            assert await session.get(mapping,1) is obj
            obj.value=None;await session.flush();await session.rollback();assert obj.value==exact
        assert (await db.one(select_row(table)))['value']==exact
    assert native.execute(f'SELECT (value).amount::text,(value).clock::text FROM "{s}".composites').fetchone()==('0.0000000000000000001','00:00:00.000001')
