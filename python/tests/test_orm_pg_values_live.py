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
