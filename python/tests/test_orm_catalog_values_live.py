from dataclasses import dataclass
from decimal import Decimal
import pytest
from neutron.orm import AsyncDatabase,AsyncSession,ColumnSpec,Database,ModelMapping,Mutation,OrmError,PgDomain,PgEnum,Session,Table,alias,exists,field,insert,query_from,select,select_row
from .test_orm_live import live_table

@dataclass
class CatalogRow:
    id: int
    state: PgEnum
    amount: PgDomain[Decimal]

@pytest.fixture
def catalog_fixture(live_table):
    url,base,native=live_table
    s=base.schema;quoted='"'+s.replace('"','""')+'"'
    native.execute(f"CREATE TYPE {quoted}.state_kind AS ENUM ('','ready','quoted''label')")
    native.execute(f'CREATE DOMAIN {quoted}.positive_amount AS numeric NOT NULL CHECK (VALUE>0)')
    native.execute(f'CREATE DOMAIN {quoted}.nested_amount AS {quoted}.positive_amount')
    native.execute(f'CREATE TYPE {quoted}.opaque_tuple AS (n bigint,label text)')
    native.execute(f'CREATE TABLE {quoted}.catalog_values(id int PRIMARY KEY,state {quoted}.state_kind NOT NULL,amount {quoted}.nested_amount)')
    native.execute(f'CREATE TABLE {quoted}.opaque_values(id int PRIMARY KEY,value {quoted}.opaque_tuple NOT NULL)')
    shadow=s+'_shadow';qshadow='"'+shadow.replace('"','""')+'"'
    native.execute(f'CREATE SCHEMA {qshadow}')
    native.execute(f"CREATE TYPE {qshadow}.state_kind AS ENUM ('','ready')")
    native.execute(f'CREATE DOMAIN {qshadow}.nested_amount AS numeric NOT NULL CHECK (VALUE>10)')
    try: yield url,s,native
    finally: native.execute(f'DROP SCHEMA {qshadow} CASCADE')


def test_native_qualified_enum_domain_precision_constraints_and_identity(catalog_fixture):
    url,s,native=catalog_fixture
    with Database.connect(url) as db:
        db.execute(Mutation("SELECT pg_catalog.set_config('search_path','pg_catalog',false)",()))
        state=db.enum_spec(s,'state_kind');amount=db.domain_spec(s,'nested_amount',ColumnSpec(Decimal,'numeric'))
        assert state.native_type is not None and amount.native_type is not None
        table=db.catalog_table('catalog_values',{'id':ColumnSpec(int,'int4'),'state':state,'amount':amount},schema=s)
        assert db.enum_spec(s,'state_kind').native_type is state.native_type
        shadow_state=db.enum_spec(s+'_shadow','state_kind');shadow_amount=db.domain_spec(s+'_shadow','nested_amount',ColumnSpec(Decimal,'numeric'))
        assert shadow_state.native_type is not None and shadow_amount.native_type is not None
        with pytest.raises(ValueError): state.check(PgEnum('ready',shadow_state.native_type))
        with pytest.raises(ValueError): amount.check(PgDomain(Decimal(20),shadow_amount.native_type))
        with pytest.raises(ValueError): db.catalog_table('catalog_values',{'id':ColumnSpec(int,'int4'),'amount':shadow_amount},schema=s)
        mapping=ModelMapping(CatalogRow,table,dict(table.columns),primary_key=('id',))
        precise=Decimal('9007199254740993.12345678901234567890')
        with Session(db) as session:
            obj=CatalogRow(1,PgEnum("quoted'label",state.native_type),PgDomain(precise,amount.native_type));session.add(mapping,obj);session.commit()
            assert session.get(mapping,1) is obj
            obj.state=PgEnum('',state.native_type);obj.amount=PgDomain(Decimal('1.25'),amount.native_type)
            session.flush();session.rollback()
            assert obj.amount.value==precise and obj.state.label=="quoted'label"
            obj.amount=PgDomain(Decimal('-1'),amount.native_type)
            with pytest.raises(OrmError) as denied: session.flush()
            assert denied.value.sqlstate=='23514'
            session.rollback();assert obj.amount.value==precise
        loaded=db.one(select_row(table));assert loaded['state'].label=="quoted'label" and loaded['amount'].value==precise
        a=alias(table,'source')
        assert db.one(query_from(a).select(field(a.column('state',PgEnum)))).identity is state.native_type
        with pytest.raises(ValueError): db.catalog_table('catalog_values',{'id':ColumnSpec(int,'int4'),'state':ColumnSpec(str,'text')},schema=s)
        with pytest.raises(ValueError): db.domain_spec(s,'nested_amount',ColumnSpec(str,'text'))
        with pytest.raises(ValueError): db.enum_spec(s,'opaque_tuple')
        with pytest.raises(ValueError): db.catalog_table('opaque_values',{'id':ColumnSpec(int,'int4'),'value':ColumnSpec(str,'text')},schema=s)
        with Database.connect(url) as other:
            with pytest.raises(ValueError,match='another connection'): other.one(select_row(table))
            outer=Table('catalog_values',{'id':ColumnSpec(int,'int4')},schema=s)
            nested=query_from(outer).select(field(outer.column('id',int))).where(exists(query_from(table).select(field(table.column('id',int)))))
            with pytest.raises(ValueError,match='another connection'): other.all(nested)
    row=native.execute(f'SELECT state::text,amount::text,pg_typeof(amount)::text FROM "{s}".catalog_values').fetchone()
    assert row[0:2]==("quoted'label",str(precise))
    assert row[2].endswith('.nested_amount')

@pytest.mark.asyncio
async def test_native_async_catalog_values_empty_enum_and_rollback(catalog_fixture):
    url,s,native=catalog_fixture
    async with await AsyncDatabase.connect(url) as db:
        state=await db.enum_spec(s,'state_kind');amount=await db.domain_spec(s,'nested_amount',ColumnSpec(Decimal,'numeric'))
        assert state.native_type is not None and amount.native_type is not None
        table=await db.catalog_table('catalog_values',{'id':ColumnSpec(int,'int4'),'state':state,'amount':amount},schema=s)
        mapping=ModelMapping(CatalogRow,table,dict(table.columns),primary_key=('id',))
        async with AsyncSession(db) as session:
            obj=CatalogRow(1,PgEnum('',state.native_type),PgDomain(Decimal('1.0000000000000001'),amount.native_type));session.add(mapping,obj);await session.commit()
            obj.state=PgEnum('unknown',state.native_type)
            with pytest.raises(OrmError) as denied: await session.flush()
            assert denied.value.sqlstate=='22P02'
            await session.rollback();assert obj.state.label==''
        assert (await db.one(select_row(table)))['amount'].value==Decimal('1.0000000000000001')
    assert native.execute(f'SELECT state::text,amount::text FROM "{s}".catalog_values').fetchone()==('', '1.0000000000000001')
