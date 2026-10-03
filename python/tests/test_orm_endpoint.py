import asyncio
from types import SimpleNamespace
import pytest
from neutron.orm import AsyncDatabase, Database, OrmError
from neutron.orm.endpoint import admit, startup_version


def test_identity_admission_is_conservative_and_not_topology_attestation():
    identity=admit('17.6 (Debian 17.6-1)','PostgreSQL 17.6 on x86_64, compiled by gcc, 64-bit')
    assert identity.engine=='postgresql' and identity.version=='17.6'
    assert identity.topology=='caller-declared-direct'
    for startup,version in (
        ('16.0 (Nucleus)','PostgreSQL 16.0'),('16.0','PostgreSQL 16.0 (Nucleus 0.1)'),
        ('17.6','PostgreSQL 16.6'),('17.6','CockroachDB v24.1'),
        (None,'PostgreSQL 17.6'),('17.6',None),('unknown','PostgreSQL 17.6'),
        ('17.6','PostgreSQL 17.6 YugabyteDB'),('18beta1','PostgreSQL 18beta1'),
    ):
        with pytest.raises(OrmError): admit(startup,version)

class IdentityConnection:
    def __init__(self,startup,reported):
        from psycopg import adapters
        from psycopg.adapt import AdaptersMap
        self.adapters=AdaptersMap(adapters)
        self.info=SimpleNamespace(parameter_status=lambda name: startup)
        self.pgconn=SimpleNamespace(finish=self.finish)
        self.finished=False;self.reported=reported;self.statements=[]
    def finish(self): self.finished=True
    def cursor(self): return self
    def __enter__(self): return self
    def __exit__(self,*args): return None
    def execute(self,sql): self.statements.append(sql)
    def fetchone(self): return {'orm_endpoint_version':self.reported}

class AsyncIdentityConnection(IdentityConnection):
    async def __aenter__(self): return self
    async def __aexit__(self,*args): return None
    async def execute(self,sql): self.statements.append(sql)
    async def fetchone(self): return {'orm_endpoint_version':self.reported}

@pytest.mark.parametrize('startup,reported',[
    ('16.0 (Nucleus)','PostgreSQL 16.0'),('17.6','PostgreSQL 16.6'),
    ('17.6','CockroachDB v24.1'),('17.6',None),
])
def test_sync_connect_refuses_and_discards_before_handoff(monkeypatch,startup,reported):
    import psycopg
    connection=IdentityConnection(startup,reported)
    monkeypatch.setattr(psycopg,'connect',lambda *a,**kw: connection)
    with pytest.raises(OrmError): Database.connect('postgresql://user:secret@host/db')
    assert connection.finished
    if 'Nucleus' in startup: assert connection.statements==[]

@pytest.mark.asyncio
@pytest.mark.parametrize('startup,reported',[
    ('16.0 (Nucleus)','PostgreSQL 16.0'),('17.6','PostgreSQL 16.6'),
    ('17.6','CockroachDB v24.1'),('17.6',None),
])
async def test_async_connect_refuses_and_discards(monkeypatch,startup,reported):
    import psycopg
    connection=AsyncIdentityConnection(startup,reported)
    async def connect(*a,**kw): return connection
    monkeypatch.setattr(psycopg.AsyncConnection,'connect',connect)
    with pytest.raises(OrmError): await AsyncDatabase.connect('postgresql://user:secret@host/db')
    assert connection.finished

@pytest.mark.asyncio
async def test_async_cancel_during_identity_probe_discards(monkeypatch):
    import psycopg
    connection=AsyncIdentityConnection('17.6','PostgreSQL 17.6')
    async def execute(sql): raise asyncio.CancelledError()
    connection.execute=execute
    async def connect(*a,**kw): return connection
    monkeypatch.setattr(psycopg.AsyncConnection,'connect',connect)
    with pytest.raises(asyncio.CancelledError): await AsyncDatabase.connect('unused')
    assert connection.finished


def test_sync_connect_hands_out_only_admitted_identity(monkeypatch):
    import psycopg
    import psycopg.types.json
    connection=IdentityConnection('17.6','PostgreSQL 17.6 on x86_64')
    monkeypatch.setattr(psycopg,'connect',lambda *a,**kw: connection)
    monkeypatch.setattr(psycopg.types.json,'set_json_loads',lambda *a,**kw: None)
    db=Database.connect('unused')
    assert db.endpoint_identity is not None and db.endpoint_identity.version=='17.6'
    assert not connection.finished
    with pytest.raises(AttributeError): db.endpoint_identity=None

@pytest.mark.asyncio
async def test_async_connect_hands_out_only_admitted_identity(monkeypatch):
    import psycopg
    import psycopg.types.json
    connection=AsyncIdentityConnection('17.6','PostgreSQL 17.6 on x86_64')
    async def connect(*a,**kw): return connection
    monkeypatch.setattr(psycopg.AsyncConnection,'connect',connect)
    monkeypatch.setattr(psycopg.types.json,'set_json_loads',lambda *a,**kw: None)
    db=await AsyncDatabase.connect('unused')
    assert db.endpoint_identity is not None and db.endpoint_identity.version=='17.6'
    assert not connection.finished

from neutron.orm.endpoint import NUCLEUS_CANDIDATE_PROFILE, guard_finite_operation

NUCLEUS_REPORTED='PostgreSQL 16.0 (Nucleus 1.2.0 — The Definitive Database)'

def candidate_identity():
    return admit('16.0 (Nucleus)',NUCLEUS_REPORTED,profile=NUCLEUS_CANDIDATE_PROFILE)


def test_nucleus_named_candidate_is_uncertified_and_exact():
    from dataclasses import FrozenInstanceError
    identity=candidate_identity()
    assert identity.engine=='nucleus' and identity.version=='1.2.0'
    assert identity.profile==NUCLEUS_CANDIDATE_PROFILE
    assert not identity.package_enabled and identity.qualification=='uncertified-finite-candidate'
    assert 'bounded-stream' not in identity.capabilities
    with pytest.raises(FrozenInstanceError): identity.package_enabled=True
    for startup,reported in [('16.0',NUCLEUS_REPORTED),('16.0 (Nucleus)','PostgreSQL 16.0'),
        ('16.0 (Nucleus)',NUCLEUS_REPORTED.replace('1.2.0','1.2.1'))]:
        with pytest.raises(OrmError): admit(startup,reported,profile=NUCLEUS_CANDIDATE_PROFILE)
    with pytest.raises(OrmError): admit('16.0 (Nucleus)',NUCLEUS_REPORTED)


def test_finite_guard_accepts_only_bound_scalar_point_crud():
    from neutron.orm.core import Table,ColumnSpec,select,insert,update,delete
    table=Table('docs',{'id':ColumnSpec(int,'int8'),'value':ColumnSpec(str,'text')})
    key=table.column('id',int)
    for operation in [select(key).where(key.eq(1)),insert(table,{'id':1,'value':''}),
        insert(table,{'id':1}).returning(key),update(table,{'value':''},where=key.eq(1)),delete(table,where=key.eq(1))]:
        guard_finite_operation(candidate_identity(),operation)


@pytest.mark.parametrize('kind',['raw','numeric','no-point','forged-function','multiple','stream','catalog'])
def test_finite_guard_refuses_before_native_dispatch(kind):
    from decimal import Decimal
    from neutron.orm.core import Table,ColumnSpec,Mutation,Predicate,select,insert
    table=Table('docs',{'id':ColumnSpec(int,'int8')})
    key=table.column('id',int)
    connection=IdentityConnection('16.0 (Nucleus)',NUCLEUS_REPORTED)
    db=Database(connection);db._endpoint_identity=candidate_identity()
    with pytest.raises(OrmError):
        if kind=='raw': db.execute(Mutation('CREATE TABLE surprise(id int)',()))
        elif kind=='numeric': db.execute(insert(Table('n',{'v':ColumnSpec(Decimal,'numeric')}),{'v':Decimal('1.50')}))
        elif kind=='no-point': db.all(select(key))
        elif kind=='forged-function': db.all(select(key).where(Predicate('pg_cancel_backend(1)',(),frozenset({table}))))
        elif kind=='multiple': db.execute(Mutation('DELETE FROM "public"."docs" WHERE "public"."docs"."id" = %s; DELETE FROM "public"."docs"',(1,),table))
        elif kind=='stream': db.stream(select(key),batch_size=1)
        else: db.enum_spec('public','kind')
    assert connection.statements==[]


@pytest.mark.asyncio
async def test_async_finite_guard_refuses_before_transaction_or_dispatch():
    from neutron.orm.core import Table,ColumnSpec,insert,Mutation
    from decimal import Decimal
    connection=AsyncIdentityConnection('16.0 (Nucleus)',NUCLEUS_REPORTED)
    db=AsyncDatabase(connection);db._endpoint_identity=candidate_identity()
    table=Table('n',{'v':ColumnSpec(Decimal,'numeric')})
    with pytest.raises(OrmError): await db.all(insert(table,{'v':Decimal('1.50')}).returning(table.column('v',Decimal)))
    with pytest.raises(OrmError): await db.execute(Mutation('CREATE TABLE surprise(id int)',()))
    with pytest.raises(OrmError): db.stream(None,batch_size=1)
    with pytest.raises(OrmError): await db.enum_spec('public','kind')
    assert connection.statements==[]


def test_sync_connect_binds_named_candidate_and_preserves_default_refusal(monkeypatch):
    import psycopg
    import psycopg.types.json
    connection=IdentityConnection('16.0 (Nucleus)',NUCLEUS_REPORTED)
    monkeypatch.setattr(psycopg,'connect',lambda *a,**kw: connection)
    monkeypatch.setattr(psycopg.types.json,'set_json_loads',lambda *a,**kw: None)
    db=Database.connect('unused',profile=NUCLEUS_CANDIDATE_PROFILE)
    assert db.endpoint_identity==candidate_identity()
    assert connection.statements==['SELECT pg_catalog.version() AS orm_endpoint_version']


@pytest.mark.asyncio
async def test_async_connect_binds_named_candidate(monkeypatch):
    import psycopg
    import psycopg.types.json
    connection=AsyncIdentityConnection('16.0 (Nucleus)',NUCLEUS_REPORTED)
    async def connect(*a,**kw): return connection
    monkeypatch.setattr(psycopg.AsyncConnection,'connect',connect)
    monkeypatch.setattr(psycopg.types.json,'set_json_loads',lambda *a,**kw: None)
    db=await AsyncDatabase.connect('unused',profile=NUCLEUS_CANDIDATE_PROFILE)
    assert db.endpoint_identity==candidate_identity()
