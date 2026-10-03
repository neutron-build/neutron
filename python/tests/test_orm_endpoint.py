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
