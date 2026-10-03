"""Real least-privilege PostgreSQL isolation, independent of query predicates."""
import asyncio
from dataclasses import dataclass,field
import secrets
from uuid import uuid4
import pytest
from neutron.orm import AsyncDatabase,ColumnSpec,Database,Mutation,OrmError,Table,insert,select_row
from .test_orm_live import live_table

@dataclass
class RlsCase:
    url: str=field(repr=False)
    table: Table
    native: object=field(repr=False)
    role: str

@pytest.fixture
def rls_case(live_table):
    import psycopg
    from psycopg import sql
    from psycopg.conninfo import make_conninfo
    url,base,native=live_table
    role='orm_rls_'+uuid4().hex
    password=secrets.token_urlsafe(32)
    table=Table('tenant_records',{'tenant':ColumnSpec(str,'text'),'id':ColumnSpec(int,'int4'),'value':ColumnSpec(str,'text')},schema=base.schema)
    native.execute(sql.SQL('CREATE ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS PASSWORD {}').format(sql.Identifier(role),sql.Literal(password)))
    try:
        native.execute(f'CREATE TABLE {table.sql}(tenant text NOT NULL,id int NOT NULL,value text NOT NULL,PRIMARY KEY(tenant,id))')
        native.execute(f"INSERT INTO {table.sql} VALUES ('a',1,'alpha'),('b',1,'beta')")
        native.execute(f'ALTER TABLE {table.sql} ENABLE ROW LEVEL SECURITY')
        native.execute(f'ALTER TABLE {table.sql} FORCE ROW LEVEL SECURITY')
        native.execute(f"CREATE POLICY tenant_isolation ON {table.sql} USING(tenant=current_setting('app.tenant',true)) WITH CHECK(tenant=current_setting('app.tenant',true))")
        native.execute(sql.SQL('GRANT USAGE ON SCHEMA {} TO {}').format(sql.Identifier(base.schema),sql.Identifier(role)))
        native.execute(sql.SQL('GRANT SELECT,INSERT,UPDATE,DELETE ON {}.{} TO {}').format(sql.Identifier(base.schema),sql.Identifier(table.name),sql.Identifier(role)))
        private_url=make_conninfo(url,user=role,password=password)
        with psycopg.connect(private_url,autocommit=True) as oracle:
            assert oracle.execute('SELECT rolsuper,rolbypassrls FROM pg_roles WHERE rolname=current_user').fetchone()==(False,False)
            assert oracle.execute(f'SELECT tenant,id,value FROM {table.sql}').fetchall()==[]
        yield RlsCase(private_url,table,native,role)
    finally:
        native.execute(sql.SQL('DROP OWNED BY {}').format(sql.Identifier(role)))
        native.execute(sql.SQL('DROP ROLE {}').format(sql.Identifier(role)))


def tenant_command(value):
    return Mutation("SELECT set_config('app.tenant',%s,true)",(value,))


def test_real_rls_alternating_success_failure_and_no_context(rls_case):
    case=rls_case;q=select_row(case.table)
    with Database.connect(case.url) as db:
        for tenant,value in [('a','alpha'),('b','beta'),('a','alpha')]:
            with db.transaction():
                db.execute(tenant_command(tenant))
                # No ORM tenant predicate: the database enforces isolation.
                assert db.all(q)==[{'tenant':tenant,'id':1,'value':value}]
            with pytest.raises(OrmError) as denied:
                with db.transaction():
                    db.execute(tenant_command(tenant))
                    db.execute(insert(case.table,{'tenant':'b' if tenant=='a' else 'a','id':9,'value':'forbidden'}))
            assert denied.value.sqlstate=='42501'
            with db.transaction(): assert db.all(q)==[]
        with pytest.raises(RuntimeError):
            with db.transaction():
                db.execute(tenant_command('b'))
                assert db.all(q)==[{'tenant':'b','id':1,'value':'beta'}]
                raise RuntimeError('application abort')
        with db.transaction(): assert db.all(q)==[]
    assert case.native.execute(f'SELECT tenant,id,value FROM {case.table.sql} ORDER BY tenant').fetchall()==[('a',1,'alpha'),('b',1,'beta')]

@pytest.mark.asyncio
async def test_real_async_rls_cancellation_and_connection_reuse(rls_case):
    case=rls_case;q=select_row(case.table)
    async with await AsyncDatabase.connect(case.url) as db:
        for tenant,value in [('a','alpha'),('b','beta')]:
            async with db.transaction():
                await db.execute(tenant_command(tenant))
                assert await db.all(q)==[{'tenant':tenant,'id':1,'value':value}]
            async with db.transaction(): assert await db.all(q)==[]
        entered=asyncio.Event()
        async def tenant_request():
            async with db.transaction():
                await db.execute(tenant_command('a'))
                assert await db.all(q)==[{'tenant':'a','id':1,'value':'alpha'}]
                entered.set();await asyncio.Event().wait()
        task=asyncio.create_task(tenant_request());await entered.wait();task.cancel()
        with pytest.raises(asyncio.CancelledError): await task
        assert not db.closed
        async with db.transaction(): assert await db.all(q)==[]
        async with db.transaction():
            await db.execute(tenant_command('b'))
            assert await db.all(q)==[{'tenant':'b','id':1,'value':'beta'}]
    assert case.native.execute(f'SELECT count(*) FROM {case.table.sql}').fetchone()==(2,)
