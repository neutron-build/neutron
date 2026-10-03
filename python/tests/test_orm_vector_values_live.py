from dataclasses import dataclass
import struct
import pytest
from neutron.orm import AsyncDatabase,AsyncSession,ColumnSpec,Database,ModelMapping,Mutation,OrmError,PgVector,Session,Table,insert,select,select_row
from .test_orm_live import live_table

@dataclass
class VectorRow:
    id: int
    value: PgVector
    optional: PgVector|None

@pytest.fixture
def vectors(live_table):
    url,base,native=live_table;s=base.schema
    native.execute(f'CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA "{s}"')
    extension_schema=native.execute("SELECT n.nspname FROM pg_catalog.pg_extension e JOIN pg_catalog.pg_namespace n ON n.oid OPERATOR(pg_catalog.=) e.extnamespace WHERE e.extname OPERATOR(pg_catalog.=) 'vector'").fetchone()[0]
    quoted='"'+extension_schema.replace('"','""')+'"'
    native.execute(f'CREATE TABLE "{s}".vectors(id int PRIMARY KEY,value {quoted}.vector(3) NOT NULL,optional {quoted}.vector)')
    native.execute(f'CREATE TYPE "{s}".vector_impostor AS (x int)')
    return url,s,extension_schema,native


def test_native_vector_extension_binary_precision_null_dimension_and_owner(vectors):
    url,s,extension_schema,native=vectors
    with Database.connect(url) as db:
        db.execute(Mutation("SELECT pg_catalog.set_config('search_path','pg_catalog',false)",()))
        spec=db.vector_spec(extension_schema);assert spec.native_type is not None
        optional=db.vector_spec(extension_schema,nullable=True)
        table=db.catalog_table('vectors',{'id':ColumnSpec(int,'int4'),'value':spec,'optional':optional},schema=s)
        exact=PgVector((struct.unpack('!f',bytes.fromhex('00000001'))[0],struct.unpack('!f',bytes.fromhex('7f7fffff'))[0],-0.5),spec.native_type)
        row=db.one(insert(table,{'id':1,'value':exact,'optional':None}).returning_row());assert row['value']==exact and row['optional'] is None
        assert db.one(select(table.column('value',PgVector)).where(table.column('value',PgVector).eq(exact)))==exact
        assert db.one(select(table.column('id',int)).where(table.column('value',PgVector).in_((exact,))))==1
        with pytest.raises(ValueError): db.vector_spec(s,'vector_impostor')
        with pytest.raises(ValueError): Table('bad',{'value':spec})
        with Database.connect(url) as other:
            with pytest.raises(ValueError,match='another connection'): other.one(select_row(table))
        mapping=ModelMapping(VectorRow,table,dict(table.columns),primary_key=('id',))
        with Session(db) as session:
            obj=session.get(mapping,1);assert obj is not None
            obj.value=PgVector.from_values((0.1,0.2,0.3),spec.native_type);obj.optional=exact;session.flush();session.rollback()
            assert obj.value==exact and obj.optional is None
            obj.value=PgVector((1.0,2.0),spec.native_type)
            with pytest.raises(OrmError): session.flush()
            session.rollback();assert obj.value==exact
    binary=native.execute(f'SELECT "{extension_schema}".vector_send(value),optional IS NULL FROM "{s}".vectors').fetchone()
    assert bytes(binary[0])==struct.pack('!HH',3,0)+struct.pack('!fff',*exact.elements) and binary[1] is True

@pytest.mark.asyncio
async def test_native_async_vector_mapping_precision_and_rollback(vectors):
    url,s,extension_schema,native=vectors
    async with await AsyncDatabase.connect(url) as db:
        spec=await db.vector_spec(extension_schema);assert spec.native_type is not None
        optional=await db.vector_spec(extension_schema,nullable=True)
        table=await db.catalog_table('vectors',{'id':ColumnSpec(int,'int4'),'value':spec,'optional':optional},schema=s)
        mapping=ModelMapping(VectorRow,table,dict(table.columns),primary_key=('id',))
        exact=PgVector.from_values((0.1,0.2,0.3),spec.native_type)
        async with AsyncSession(db) as session:
            obj=VectorRow(1,exact,None);session.add(mapping,obj);await session.commit()
            assert await session.get(mapping,1) is obj
            obj.optional=PgVector((1.0,),spec.native_type);await session.flush();await session.rollback();assert obj.optional is None
        assert (await db.one(select_row(table)))['value']==exact
    native_components=native.execute(f'SELECT value::real[] FROM "{s}".vectors').fetchone()[0]
    assert tuple(native_components)==exact.elements
