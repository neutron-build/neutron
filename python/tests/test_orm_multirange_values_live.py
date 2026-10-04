from dataclasses import dataclass
import datetime as dt
from decimal import Decimal
import os
import pytest
from neutron.orm import AsyncDatabase,AsyncSession,ColumnSpec,Database,ModelMapping,Mutation,OrmError,PgMultirange,PgRange,Session,Table,insert,select,select_row
from .test_orm_live import live_table

UTC=dt.timezone.utc

@dataclass
class MultirangeRow:
    id: int
    i4: PgMultirange
    i8: PgMultirange|None

@pytest.fixture
def multiranges(live_table):
    url,base,native=live_table;s=base.schema
    if int(native.execute('SHOW server_version_num').fetchone()[0])<140000:
        if os.environ.get('NEUTRON_LIVE_REQUIRED')=='1': pytest.fail('PostgreSQL 14+ is required for multirange tests')
        pytest.skip('PostgreSQL 14+ required for multiranges')
    native.execute("SET TIME ZONE 'UTC'")
    native.execute(f'CREATE TABLE "{s}".mr(id int PRIMARY KEY,i4 int4multirange NOT NULL,i8 int8multirange,n nummultirange,ts tsmultirange,tstz tstzmultirange,d datemultirange,CONSTRAINT bounded_upper CHECK (coalesce(upper(i4),0)<1000))')
    native.execute(f'CREATE TYPE "{s}".float_range AS RANGE (subtype=float8,multirange_type_name="{s}".float_multirange)')
    native.execute(f'CREATE TABLE "{s}".user_mr(id int PRIMARY KEY,v "{s}".float_multirange)')
    native.execute(f'CREATE DOMAIN "{s}".int4_domain AS int4multirange')
    native.execute(f'CREATE TYPE "{s}".mr_composite AS (m int4multirange)')
    return url,s,native

def r(lower,upper,li=True,ui=False): return PgRange(lower,upper,li and lower is not None,ui and upper is not None)

def sync_specs(db):
    return {'id':ColumnSpec(int,'int4'),'i4':db.multirange_spec('int4multirange'),'i8':db.multirange_spec('int8multirange',nullable=True),'n':db.multirange_spec('nummultirange',nullable=True),'ts':db.multirange_spec('tsmultirange',nullable=True),'tstz':db.multirange_spec('tstzmultirange',nullable=True),'d':db.multirange_spec('datemultirange',nullable=True)}

async def async_specs(db):
    return {'id':ColumnSpec(int,'int4'),'i4':await db.multirange_spec('int4multirange'),'i8':await db.multirange_spec('int8multirange',nullable=True),'n':await db.multirange_spec('nummultirange',nullable=True),'ts':await db.multirange_spec('tsmultirange',nullable=True),'tstz':await db.multirange_spec('tstzmultirange',nullable=True),'d':await db.multirange_spec('datemultirange',nullable=True)}

def values(specs):
    ident={name:spec.native_type for name,spec in specs.items() if spec.native_type is not None}
    full={'id':1,
        'i4':PgMultirange((r(1,3),r(5,8)),ident['i4']),
        'i8':PgMultirange((r(2**40,2**41),),ident['i8']),
        'n':PgMultirange((PgRange(Decimal('1.10'),Decimal('2.0'),True,False),PgRange(Decimal('2.0'),Decimal('3'),False,True)),ident['n']),
        'ts':PgMultirange((r(dt.datetime(2020,1,1),dt.datetime(2020,1,2)),),ident['ts']),
        'tstz':PgMultirange((r(dt.datetime(2020,1,1,tzinfo=UTC),dt.datetime(2020,1,2,tzinfo=UTC)),),ident['tstz']),
        'd':PgMultirange((r(dt.date(2020,1,1),dt.date(2020,1,5)),r(dt.date(2020,2,1),dt.date(2020,2,3))),ident['d'])}
    empty={'id':2,'i4':PgMultirange((),ident['i4']),'i8':None,'n':None,'ts':None,'tstz':None,'d':None}
    unbounded={'id':3,'i4':PgMultirange((r(None,3),r(5,None)),ident['i4']),'i8':PgMultirange((),ident['i8']),'n':PgMultirange((PgRange(None,None),),ident['n']),'ts':None,'tstz':None,'d':None}
    return full,empty,unbounded

FULL_ORACLE="""SELECT i4='{[1,3),[5,8)}'::int4multirange,i8='{[1099511627776,2199023255552)}'::int8multirange,n='{[1.10,2.0),(2.0,3]}'::nummultirange,n::text='{[1.10,2.0),(2.0,3]}',ts='{[2020-01-01 00:00:00,2020-01-02 00:00:00)}'::tsmultirange,tstz='{[2020-01-01 00:00:00+00,2020-01-02 00:00:00+00)}'::tstzmultirange,d='{[2020-01-01,2020-01-05),[2020-02-01,2020-02-03)}'::datemultirange FROM "%s".mr WHERE id=1"""


def test_native_multirange_roundtrip_null_empty_unbounded_and_independent_oracles(multiranges):
    url,s,native=multiranges
    with Database.connect(url) as db:
        db.execute(Mutation("SELECT pg_catalog.set_config('search_path','pg_catalog',false)",()))
        specs=sync_specs(db);table=db.catalog_table('mr',specs,schema=s)
        full,empty,unbounded=values(specs)
        for row in (full,empty,unbounded): assert db.execute(insert(table,row))==1
        for row in (full,empty,unbounded):
            got=db.one(select_row(table).where(table.column('id',int).eq(row['id'])))
            assert got==row
        i4=table.column('i4',PgMultirange)
        assert db.one(select(table.column('id',int)).where(i4.eq(full['i4'])))==1
        assert db.one(select(table.column('id',int)).where(i4.eq(empty['i4'])))==2
        assert sorted(db.all(select(table.column('id',int)).where(i4.in_((full['i4'],unbounded['i4'])))))==[1,3]
        assert db.all(select(table.column('id',int)).where(i4.eq(PgMultirange((r(1,3),),full['i4'].identity))))==[]
        assert sorted(db.all(select(table.column('id',int)).where(table.nullable_column('i8',PgMultirange).eq(None))))==[2]
        # Empty multirange and SQL NULL stay distinct.
        assert db.one(select(table.nullable_column('i8',PgMultirange)).where(table.column('id',int).eq(3)))==unbounded['i8']
        assert db.one(select(table.nullable_column('i8',PgMultirange)).where(table.column('id',int).eq(2))) is None
        with db.stream(select(table.column('i4',PgMultirange)).where(table.column('id',int).eq(1)),batch_size=1) as stream:
            assert list(stream)==[full['i4']]
    assert native.execute(FULL_ORACLE%s).fetchone()==(True,)*7
    assert native.execute(f'SELECT i4::text,i8 IS NULL,n IS NULL FROM "{s}".mr WHERE id=2').fetchone()==('{}',True,True)
    assert native.execute(f'SELECT i4::text,i8::text,n::text FROM "{s}".mr WHERE id=3').fetchone()==('{(,3),[5,)}','{}','{(,)}')
    with native.cursor(binary=True) as oracle:
        oracle.execute(f'SELECT i4 FROM "{s}".mr WHERE id=1')
        assert [(item.lower,item.upper,item.bounds) for item in oracle.fetchone()[0]]==[(1,3,'[)'),(5,8,'[)')]


def test_native_server_normalization_agrees_with_explicit_from_ranges_and_strict_constructor(multiranges):
    url,s,native=multiranges
    native.execute(f"""INSERT INTO "{s}".mr(id,i4,i8,n,d) VALUES (10,'{{(1,5),[7,9],[9,12],[20,21)}}','{{}}','{{[1,2),[2,3),(3,4),(4,5]}}','{{[2020-01-01,2020-01-03],[2020-01-04,2020-01-06)}}')""")
    with Database.connect(url) as db:
        specs=sync_specs(db);table=db.catalog_table('mr',specs,schema=s)
        row=db.one(select_row(table))
        i4,n,d=specs['i4'].native_type,specs['n'].native_type,specs['d'].native_type
        assert row['i4']==PgMultirange.from_ranges((PgRange(1,5,False,False),PgRange(7,9,True,True),PgRange(9,12,True,True),PgRange(20,21,True,False)),i4)==PgMultirange((r(2,5),r(7,13),r(20,21)),i4)
        dec=Decimal
        assert row['n']==PgMultirange.from_ranges((PgRange(dec(1),dec(2),True,False),PgRange(dec(2),dec(3),True,False),PgRange(dec(3),dec(4),False,False),PgRange(dec(4),dec(5),False,True)),n)
        assert row['n']==PgMultirange((PgRange(dec(1),dec(3),True,False),PgRange(dec(3),dec(4),False,False),PgRange(dec(4),dec(5),False,True)),n)
        assert row['d']==PgMultirange.from_ranges((PgRange(dt.date(2020,1,1),dt.date(2020,1,3),True,True),PgRange(dt.date(2020,1,4),dt.date(2020,1,6),True,False)),d)
        # The strict constructor refuses the unnormalized form the server would have merged.
        with pytest.raises(ValueError): PgMultirange((PgRange(1,5,True,True),PgRange(6,8,True,False)),i4)
        # A semantically equal but unnormalized client value is stored by the server in its canonical form.
        db.execute(insert(table,{'id':11,'i4':PgMultirange.from_ranges((PgRange(1,3,True,True),PgRange(4,6,True,True)),i4)}))
        assert db.one(select(table.column('i4',PgMultirange)).where(table.column('id',int).eq(11)))==PgMultirange((r(1,7),),i4)
    assert native.execute(f'SELECT i4::text FROM "{s}".mr WHERE id=11').fetchone()==('{[1,7)}',)


def test_native_multirange_server_refusal_rollback_session_and_owner_affinity(multiranges):
    url,s,native=multiranges
    with Database.connect(url) as db:
        specs=sync_specs(db);table=db.catalog_table('mr',specs,schema=s)
        full,empty,unbounded=values(specs)
        db.execute(insert(table,full))
        i4=specs['i4'].native_type;assert i4 is not None
        with pytest.raises(OrmError) as refused: db.execute(insert(table,{'id':50,'i4':PgMultirange((r(2000,3000),),i4)}))
        assert refused.value.sqlstate=='23514'
        with pytest.raises(RuntimeError):
            with db.transaction():
                db.execute(insert(table,{'id':60,'i4':full['i4']}));raise RuntimeError('business failure')
        assert native.execute(f'SELECT count(*) FROM "{s}".mr WHERE id IN (50,60)').fetchone()==(0,)
        small=db.catalog_table('mr',{'id':specs['id'],'i4':specs['i4'],'i8':specs['i8']},schema=s)
        mapping=ModelMapping(MultirangeRow,small,dict(small.columns),primary_key=('id',))
        with Session(db) as session:
            obj=session.get(mapping,1);assert obj is not None and obj.i4==full['i4'] and obj.i8==full['i8']
            obj.i4=unbounded['i4'];obj.i8=None;session.flush()
            assert native.execute(f"SELECT i4='{{(,3),[5,)}}'::int4multirange,i8 IS NULL FROM \"{s}\".mr WHERE id=1").fetchone()==(True,True)
            session.rollback();assert obj.i4==full['i4'] and obj.i8==full['i8']
            obj.i4=PgMultirange((r(2000,3000),),i4)
            with pytest.raises(OrmError): session.flush()
            session.rollback();assert obj.i4==full['i4']
            obj.i4=empty['i4'];session.commit()
        assert native.execute(f'SELECT i4::text,i8 IS NOT NULL FROM "{s}".mr WHERE id=1').fetchone()==('{}',True)
        with Database.connect(url) as other:
            with pytest.raises(ValueError,match='another connection'): other.one(select_row(table))
            foreign=other.multirange_spec('int4multirange').native_type
            assert foreign is not None
            with pytest.raises(ValueError): insert(table,{'id':70,'i4':PgMultirange((),foreign)})
            with pytest.raises(ValueError): Table('bad',{'i4':specs['i4']})
    assert native.execute(f'SELECT count(*) FROM "{s}".mr WHERE id=70').fetchone()==(0,)


def test_native_multirange_refuses_unproven_user_defined_domain_composite_and_mismatched_types(multiranges):
    url,s,native=multiranges
    native.execute(f"INSERT INTO \"{s}\".user_mr VALUES (1,'{{[1.5,2.5)}}')")
    with Database.connect(url) as db:
        specs=sync_specs(db)
        for name in ('float_multirange','int4range','int4multirange[]','text','int2multirange'):
            with pytest.raises(ValueError): db.multirange_spec(name)
        with pytest.raises(ValueError): db.enum_spec('pg_catalog','int4multirange')
        with pytest.raises(ValueError): db.domain_spec(s,'int4_domain',specs['i4'])
        with pytest.raises(ValueError): db.composite_spec(s,'mr_composite',{'m':specs['i4']})
        with pytest.raises(ValueError): db.catalog_table('user_mr',{'id':specs['id'],'v':specs['i4']},schema=s)
        with pytest.raises(ValueError): db.catalog_table('mr',{'id':specs['id'],'i4':db.multirange_spec('int8multirange')},schema=s)
        with pytest.raises(ValueError): db.catalog_table('mr',{'id':specs['id'],'i4':ColumnSpec(str,'text')},schema=s)
        with pytest.raises(ValueError): ColumnSpec(PgMultirange,'int4multirange')
    with Database.connect(url) as db:
        # A user-defined multirange read through an inadmissible text profile is refused by native result OID identity.
        plain=Table('user_mr',{'id':ColumnSpec(int,'int4'),'v':ColumnSpec(str,'text')},schema=s)
        with pytest.raises(OrmError): db.all(select_row(plain))


@pytest.mark.asyncio
async def test_native_async_multirange_roundtrip_null_empty_rollback_mapping_and_refusals(multiranges):
    url,s,native=multiranges
    async with await AsyncDatabase.connect(url) as db:
        await db.execute(Mutation("SELECT pg_catalog.set_config('search_path','pg_catalog',false)",()))
        specs=await async_specs(db);table=await db.catalog_table('mr',specs,schema=s)
        full,empty,unbounded=values(specs)
        for row in (full,empty,unbounded): assert await db.execute(insert(table,row))==1
        for row in (full,empty,unbounded): assert await db.one(select_row(table).where(table.column('id',int).eq(row['id'])))==row
        i4=table.column('i4',PgMultirange)
        assert await db.one(select(table.column('id',int)).where(i4.eq(empty['i4'])))==2
        assert sorted(await db.all(select(table.column('id',int)).where(i4.in_((full['i4'],unbounded['i4'])))))==[1,3]
        assert await db.one(select(table.nullable_column('i8',PgMultirange)).where(table.column('id',int).eq(2))) is None
        with pytest.raises(OrmError) as refused: await db.execute(insert(table,{'id':50,'i4':PgMultirange((r(2000,3000),),full['i4'].identity)}))
        assert refused.value.sqlstate=='23514'
        with pytest.raises(RuntimeError):
            async with db.transaction():
                await db.execute(insert(table,{'id':60,'i4':full['i4']}));raise RuntimeError('business failure')
        small=await db.catalog_table('mr',{'id':specs['id'],'i4':specs['i4'],'i8':specs['i8']},schema=s)
        mapping=ModelMapping(MultirangeRow,small,dict(small.columns),primary_key=('id',))
        async with AsyncSession(db) as session:
            obj=await session.get(mapping,1);assert obj is not None and obj.i4==full['i4']
            obj.i4=unbounded['i4'];obj.i8=None;await session.flush();await session.rollback()
            assert obj.i4==full['i4'] and obj.i8==full['i8']
            obj.i4=PgMultirange((r(2000,3000),),full['i4'].identity)
            with pytest.raises(OrmError): await session.flush()
            await session.rollback();assert obj.i4==full['i4']
        async with await AsyncDatabase.connect(url) as other:
            with pytest.raises(ValueError,match='another connection'): await other.one(select_row(table))
            foreign=(await other.multirange_spec('int4multirange')).native_type
            assert foreign is not None
            with pytest.raises(ValueError): insert(table,{'id':70,'i4':PgMultirange((),foreign)})
        for name in ('float_multirange','int4range'):
            with pytest.raises(ValueError): await db.multirange_spec(name)
        with pytest.raises(ValueError): await db.domain_spec(s,'int4_domain',specs['i4'])
        with pytest.raises(ValueError): await db.composite_spec(s,'mr_composite',{'m':specs['i4']})
        with pytest.raises(ValueError): await db.catalog_table('user_mr',{'id':specs['id'],'v':specs['i4']},schema=s)
    assert native.execute(FULL_ORACLE%s).fetchone()==(True,)*7
    assert native.execute(f'SELECT count(*) FROM "{s}".mr WHERE id IN (50,60,70)').fetchone()==(0,)
    with native.cursor(binary=True) as oracle:
        oracle.execute(f'SELECT i4 FROM "{s}".mr WHERE id=3')
        assert [(item.lower,item.upper,item.bounds) for item in oracle.fetchone()[0]]==[(None,3,'()'),(5,None,'[)')]
