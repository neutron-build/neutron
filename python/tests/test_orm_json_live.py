from decimal import Decimal
import pytest
from neutron.orm import AsyncDatabase, Database, JSON_NULL, JsonDocument, ColumnSpec, Table, insert, select, update
from .test_orm_live import live_table


def setup_json(parent,native):
    t=Table('documents',{'id':ColumnSpec(int,'int4'),'j':ColumnSpec(JsonDocument,'json',nullable=True),'b':ColumnSpec(JsonDocument,'jsonb',nullable=True)},schema=parent.schema)
    native.execute(f'CREATE TABLE {t.sql} (id int PRIMARY KEY,j json,b jsonb)')
    return t


def test_native_json_null_precision_and_context_isolation(live_table):
    url,parent,native=live_table;t=setup_json(parent,native)
    doc=JsonDocument('{"amount":12345678901234567890.123456789,"values":[false,0,""]}')
    with Database.connect(url) as db:
        for i,value in enumerate((None,JSON_NULL,doc),1):
            db.execute(insert(t,{'id':i,'j':value,'b':value}))
        raw=native.execute(f'SELECT id,j IS NULL,b IS NULL,j::text,b::text FROM {t.sql} ORDER BY id').fetchall()
        assert raw[0]==(1,True,True,None,None)
        assert raw[1]==(2,False,False,'null','null')
        assert JsonDocument(raw[2][3]).parsed()==doc.parsed()==JsonDocument(raw[2][4]).parsed()
        for name in ('j','b'):
            column=t.nullable_column(name,JsonDocument)
            assert db.one(select(column).where(t.column('id',int).eq(1))) is None
            assert db.one(select(column).where(t.column('id',int).eq(2)))==JSON_NULL
            assert db.one(select(column).where(t.column('id',int).eq(3))).parsed()['amount']==Decimal('12345678901234567890.123456789')
        db.execute(update(t,{'b':JSON_NULL},where=t.column('id',int).eq(1)))
        assert db.one(select(t.column('id',int)).where(t.nullable_column('b',JsonDocument).eq(JSON_NULL)).where(t.column('id',int).eq(1)))==1
    # Independent connection retains standard psycopg parser, no global adapters.
    assert native.execute(f'SELECT b FROM {t.sql} WHERE id=2').fetchone()==(None,)

@pytest.mark.asyncio
async def test_native_async_json_null(live_table):
    url,parent,native=live_table;t=setup_json(parent,native)
    async with await AsyncDatabase.connect(url) as db:
        await db.execute(insert(t,{'id':1,'b':JSON_NULL,'j':None}))
        assert await db.one(select(t.nullable_column('b',JsonDocument)))==JSON_NULL
        assert await db.one(select(t.nullable_column('j',JsonDocument))) is None
        assert native.execute(f'SELECT b IS NULL,b::text,j IS NULL FROM {t.sql}').fetchone()==(False,'null',True)
