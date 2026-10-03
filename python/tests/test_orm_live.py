"""Native PostgreSQL state is the oracle; no ORM decoder used for expectations."""
import datetime as dt
from decimal import Decimal
import os
import uuid
import pytest
from neutron.orm import AsyncDatabase, CardinalityError, ColumnSpec, DEFAULT, Database, OMIT, OrmError, Table, insert, select, select_row, update

@pytest.fixture
def live_table():
    url=os.environ.get('NEUTRON_TEST_DATABASE_URL')
    if not url:
        if os.environ.get('NEUTRON_LIVE_REQUIRED')=='1': pytest.fail('required PostgreSQL URL missing')
        pytest.skip('PostgreSQL live tests not configured')
    try:
        import psycopg
    except ImportError:
        if os.environ.get('NEUTRON_LIVE_REQUIRED')=='1': pytest.fail('required psycopg dependency missing')
        pytest.skip('optional psycopg dependency missing')
    schema='neutron_orm_'+uuid.uuid4().hex
    with psycopg.connect(url,autocommit=True) as native:
        native.execute(f'CREATE SCHEMA "{schema}"')
        try:
            native.execute(f'CREATE TABLE "{schema}".records(id int PRIMARY KEY, n int DEFAULT 17, active bool NOT NULL DEFAULT true, label text NOT NULL DEFAULT \'\', amount numeric, moment timestamptz)')
            t=Table('records',{'id':ColumnSpec(int,'int4'),'n':ColumnSpec(int,'int4',nullable=True),'active':ColumnSpec(bool,'bool'),'label':ColumnSpec(str,'text'),'amount':ColumnSpec(Decimal,'numeric',nullable=True),'moment':ColumnSpec(dt.datetime,'timestamptz',nullable=True)},schema=schema)
            yield url,t,native
        finally:
            native.execute(f'DROP SCHEMA "{schema}" CASCADE')


def test_sync_crud_native_value_oracle(live_table):
    url,t,native=live_table
    amount=Decimal('12345678901234567890.123456789')
    moment=dt.datetime(2024,1,2,3,4,5,123456,tzinfo=dt.timezone.utc)
    with Database.connect(url) as db:
        for ident,state in enumerate([OMIT,DEFAULT,None,0],1):
            assert db.execute(insert(t,{'id':ident,'n':state,'active':False,'label':'','amount':amount,'moment':moment}))==1
        rows=native.execute(f'SELECT id,n,active,label,amount::text FROM {t.sql} ORDER BY id').fetchall()
        assert rows==[(1,17,False,'',str(amount)),(2,17,False,'',str(amount)),(3,None,False,'',str(amount)),(4,0,False,'',str(amount))]
        assert db.one(select(t.nullable_column('amount',Decimal)).where(t.column('id',int).eq(1)))==amount
        assert db.one(select(t.nullable_column('moment',dt.datetime)).where(t.column('id',int).eq(1)))==moment
        for state,expected in [(42,42),(OMIT,42),(DEFAULT,17),(None,None),(0,0)]:
            assert db.execute(update(t,{'n':state,'label':'changed'},where=t.column('id',int).eq(1)))==1
            assert native.execute(f'SELECT n FROM {t.sql} WHERE id=1').fetchone()==(expected,)
        with pytest.raises(CardinalityError): db.one(select(t.column('id',int)))
        with pytest.raises(OrmError) as exc: db.execute(insert(t,{'id':1}))
        assert exc.value.sqlstate=='23505'
        assert db.one(select(t.column('id',int)).where(t.column('id',int).eq(1)))==1
        with pytest.raises(ValueError):
            with db.transaction():
                db.execute(insert(t,{'id':99}));raise ValueError('business failure')
        assert native.execute(f'SELECT count(*) FROM {t.sql} WHERE id=99').fetchone()==(0,)

@pytest.mark.asyncio
async def test_async_crud_and_rollback(live_table):
    url,t,native=live_table
    async with await AsyncDatabase.connect(url) as db:
        async with db.transaction():
            assert await db.execute(insert(t,{'id':1,'n':0,'active':False,'label':''}))==1
        assert native.execute(f'SELECT n,active,label FROM {t.sql} WHERE id=1').fetchone()==(0,False,'')
        assert await db.one(select(t.column('id',int)).where(t.column('id',int).eq(1)))==1
        assert await db.one_or_none(select(t.column('id',int)).where(t.column('id',int).eq(99))) is None
        with pytest.raises(ValueError):
            async with db.transaction():
                await db.execute(insert(t,{'id':2}));raise ValueError('business failure')
        assert native.execute(f'SELECT count(*) FROM {t.sql} WHERE id=2').fetchone()==(0,)
