import pytest
from neutron.orm import AsyncDatabase,Database,Mutation,OrmError,Predicate
from neutron.orm.sql_admission import validate_scope_sql
from .test_orm_clients import Connection,AsyncConnection
from .test_orm_stream import Q

@pytest.mark.parametrize('sql',[
    'COMMIT','/* hide /* nested */ */ ROLLBACK','BEGIN','SAVEPOINT x','RELEASE x',
    'SET LOCAL ROLE x','RESET ALL','SELECT 1;COMMIT','SELECT 1; /*comment*/ BEGIN',
    "SELECT 'unfinished",'SELECT $tag$unfinished','SELECT 1 /*unfinished',
    "SELECT 'ambiguous\\quote'",
])
def test_transaction_control_and_ambiguous_sql_refuse(sql):
    with pytest.raises(OrmError): validate_scope_sql(sql)

@pytest.mark.parametrize('sql',[
    "SELECT ';COMMIT'",'SELECT ";COMMIT"',"SELECT E'escaped\\\';COMMIT'",
    'SELECT $body$;ROLLBACK$body$', '/*nested /* ok */ */ INSERT INTO t VALUES (%s); --tail',
    'WITH t AS (SELECT 1) SELECT * FROM t','((SELECT 1) UNION (SELECT 2))',
])
def test_single_data_sql_quotes_and_nested_comments_admit(sql): validate_scope_sql(sql)


def test_native_handle_guard_and_returning_compile_cannot_settle_raw_control():
    db=Database(Connection([]));tx=db.begin()
    for sql in ('COMMIT','ROLLBACK','SELECT 1;COMMIT'):
        with pytest.raises(OrmError): db.execute(Mutation(sql,()))
    with pytest.raises(OrmError): db.one(Q.where(Predicate('TRUE;COMMIT')))
    assert tx.state=='active';tx.rollback()

@pytest.mark.asyncio
async def test_async_native_handle_guard_cannot_settle_raw_control():
    db=AsyncDatabase(AsyncConnection([]));tx=await db.begin()
    with pytest.raises(OrmError): await db.execute(Mutation('COMMIT',()))
    with pytest.raises(OrmError): await db.one(Q.where(Predicate('TRUE;COMMIT')))
    assert tx.state=='active';await tx.rollback()


def test_forged_returning_mutation_cannot_escape_implicit_transaction():
    from .test_orm_stream import T
    db=Database(Connection([]))
    forged=Mutation('COMMIT',(),T).returning(T.column('id',int))
    with pytest.raises(OrmError): db.one(forged)
    assert db._owner is None and not db.closed


def test_control_is_globally_refused_while_standalone_single_ddl_admitted():
    from .test_orm_stream import T
    db=Database(Connection([]))
    for sql in ('BEGIN','COMMIT','ROLLBACK','SAVEPOINT x','SET ROLE x','RESET ALL','CREATE TABLE x(id int);BEGIN'):
        with pytest.raises(OrmError): db.execute(Mutation(sql,()))
    assert db._owner is None
    assert db.execute(Mutation('CREATE TABLE x(id integer)',()))==1
    with pytest.raises(OrmError): db.all(Q.where(Predicate('TRUE;BEGIN')))
    assert db._owner is None
