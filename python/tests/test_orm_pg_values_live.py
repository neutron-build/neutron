from dataclasses import dataclass
import datetime as dt
from decimal import Decimal
import pytest
from neutron.orm import ArrayDimension,AsyncDatabase,AsyncSession,ColumnSpec,Database,Interval,ModelMapping,ObjectState,OrmError,PgArray,Session,Table,TimeOfDay,array_spec,insert,select,select_row
from .test_orm_live import live_table

@dataclass
class ValueRow:
    id: int
    ints: PgArray[int]|None
    clock: TimeOfDay
    span: Interval

@pytest.fixture
def value_table(live_table):
    url,base,native=live_table
    table=Table('pg_values',{'id':ColumnSpec(int,'int4'),'ints':array_spec(int,'int4',nullable=True),'texts':array_spec(str,'text',nullable=True),'numbers':array_spec(Decimal,'numeric',nullable=True),'bytes':array_spec(bytes,'bytea',nullable=True),'clock':ColumnSpec(TimeOfDay,'time'),'span':ColumnSpec(Interval,'interval'),'date':ColumnSpec(dt.date,'date')},schema=base.schema)
    native.execute(f'CREATE TABLE {table.sql}(id int PRIMARY KEY,ints int4[],texts text[],numbers numeric[],bytes bytea[],clock time NOT NULL,span interval NOT NULL,date date NOT NULL DEFAULT DATE \'2000-01-01\')')
    return url,table,native


def test_native_array_lower_bounds_nulls_empty_and_temporal_components(value_table):
    url,table,native=value_table
    ints=PgArray((ArrayDimension(2,-3),ArrayDimension(2,5)),(1,None,-2,3))
    texts=PgArray((ArrayDimension(4,0),),('', 'NULL','"\\,{}',None))
    numbers=PgArray((ArrayDimension(2),),(Decimal('123456789.000000000123'),None))
    blobs=PgArray((ArrayDimension(3),),(b'',b'\x00\xff',None))
    clock=TimeOfDay(86_400_000_000);span=Interval(14,-3,7_200_000_001)
    with Database.connect(url) as db:
        db.execute(insert(table,{'id':1,'ints':ints,'texts':texts,'numbers':numbers,'bytes':blobs,'clock':clock,'span':span}))
        row=db.one(select_row(table))
        assert (row['ints'],row['texts'],row['numbers'],row['bytes'],row['clock'],row['span'])==(ints,texts,numbers,blobs,clock,span)
        db.execute(insert(table,{'id':2,'ints':PgArray((),()),'clock':TimeOfDay(1),'span':Interval()}))
        db.execute(insert(table,{'id':3,'ints':None,'clock':TimeOfDay(0),'span':Interval()}))
        assert db.one(select(table.nullable_column('ints',PgArray[int])).where(table.column('id',int).eq(2)))==PgArray((),())
        assert db.one(select(table.nullable_column('ints',PgArray[int])).where(table.column('id',int).eq(3))) is None
        with db.stream(select(table.nullable_column('ints',PgArray[int])).where(table.column('id',int).eq(1)),batch_size=1) as stream:
            assert list(stream)==[ints]
    assert native.execute(f'SELECT array_dims(ints),ints[-3][6] IS NULL,clock::text,extract(year from span),extract(month from span),extract(day from span),extract(second from span) FROM {table.sql} WHERE id=1').fetchone()==('[-3:-2][5:6]',True,'24:00:00',Decimal(1),Decimal(2),Decimal(-3),Decimal('0.000001'))


def test_native_mapped_array_replacement_and_rollback(value_table):
    url,table,native=value_table
    mapping=ModelMapping(ValueRow,table,{name:table.columns[name] for name in ('id','ints','clock','span')},primary_key=('id',))
    initial=PgArray((ArrayDimension(2,0),),(1,None))
    with Session.connect(url) as session:
        obj=ValueRow(1,initial,TimeOfDay(123456),Interval(1,2,3));session.add(mapping,obj);session.commit()
        obj.ints=PgArray((ArrayDimension(1,-1),),(8,));obj.clock=TimeOfDay(86_400_000_000);obj.span=Interval(-2,3,-4)
        session.flush();session.rollback()
        assert obj.ints==initial and obj.clock==TimeOfDay(123456) and obj.span==Interval(1,2,3)
        assert session.object_state(obj) is ObjectState.PERSISTENT
        assert native.execute(f'SELECT array_dims(ints),clock::text FROM {table.sql}').fetchone()==('[0:1]','00:00:00.123456')

@pytest.mark.asyncio
async def test_native_async_component_codecs_and_oid_refusal(value_table):
    url,table,native=value_table
    values=PgArray((ArrayDimension(2,-1),),(None,7))
    async with await AsyncDatabase.connect(url) as db:
        result=await db.one(insert(table,{'id':1,'ints':values,'clock':TimeOfDay(1),'span':Interval(2,3,4)}).returning(table.nullable_column('ints',PgArray[int])))
        assert result==values
        assert (await db.one(select_row(table)))['span']==Interval(2,3,4)
    wrong=Table(table.name,{'id':ColumnSpec(int,'int8')},schema=table.schema)
    async with await AsyncDatabase.connect(url) as db:
        with pytest.raises(OrmError) as mismatch: await db.one(select(wrong.column('id',int)))
        assert isinstance(mismatch.value.__cause__,ValueError)
        assert db.closed
    assert native.execute(f'SELECT ints[-1] IS NULL,ints[0] FROM {table.sql}').fetchone()==(True,7)

from neutron.orm import PgRange,range_spec

@dataclass
class RangeRow:
    id: int
    value: PgRange[int]|None

@pytest.fixture
def range_table(live_table):
    url,base,native=live_table
    table=Table('pg_ranges',{'id':ColumnSpec(int,'int4'),'value':range_spec(int,'int4range',nullable=True),'number':range_spec(Decimal,'numrange',nullable=True),'dates':range_spec(dt.date,'daterange',nullable=True),'instants':range_spec(dt.datetime,'tstzrange',nullable=True)},schema=base.schema)
    native.execute(f'CREATE TABLE {table.sql}(id int PRIMARY KEY,value int4range,number numrange,dates daterange,instants tstzrange)')
    return url,table,native


def test_native_ranges_canonical_bounds_null_empty_and_precision(range_table):
    url,table,native=range_table
    number=PgRange(Decimal('1.0000000000000000001'),Decimal('9.9999999999999999999'),True,True)
    dates=PgRange(dt.date(2020,1,1),dt.date(2020,1,4),True,False)
    instants=PgRange(dt.datetime(2020,1,1,1,2,3,123456,tzinfo=dt.timezone(dt.timedelta(hours=3))),None,True,False)
    with Database.connect(url) as db:
        result=db.one(insert(table,{'id':1,'value':PgRange(1,3,True,True),'number':number,'dates':dates,'instants':instants}).returning_row())
        assert result['value']==PgRange(1,4,True,False)
        assert (result['number'],result['dates'],result['instants'])==(number,dates,instants)
        for identifier,value in ((2,None),(3,PgRange(empty=True)),(4,PgRange())):
            db.execute(insert(table,{'id':identifier,'value':value}))
            assert db.one(select(table.nullable_column('value',PgRange[int])).where(table.column('id',int).eq(identifier)))==value
    assert native.execute(f'SELECT lower(value),upper(value),lower_inc(value),upper_inc(value),number::text FROM {table.sql} WHERE id=1').fetchone()==(1,4,True,False,'[1.0000000000000000001,9.9999999999999999999]')
    assert native.execute(f'SELECT id,value IS NULL,isempty(value),lower_inf(value),upper_inf(value) FROM {table.sql} WHERE id>1 ORDER BY id').fetchall()==[(2,True,None,None,None),(3,False,True,False,False),(4,False,False,True,True)]

@pytest.mark.asyncio
async def test_native_async_mapped_range_canonicalization_and_rollback(range_table):
    url,table,native=range_table
    mapping=ModelMapping(RangeRow,table,{'id':table.columns['id'],'value':table.columns['value']},primary_key=('id',))
    async with await AsyncSession.connect(url) as session:
        obj=RangeRow(1,PgRange(1,3,True,True));session.add(mapping,obj);await session.commit()
        assert obj.value==PgRange(1,4,True,False)
        obj.value=PgRange(empty=True);await session.flush();await session.rollback()
        assert obj.value==PgRange(1,4,True,False)
        assert native.execute(f'SELECT value::text FROM {table.sql}').fetchone()==('[1,4)',)
