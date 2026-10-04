#!/usr/bin/env python3
"""Fresh-installed finite Python admission facts with a PostgreSQL oracle.

Coordinator supplies owned endpoint URLs through environment variables and the
exact executed Nucleus binary hash. Provisioning uses raw psycopg, never ORM DDL.
This bounded gate has no timing, soak, package enablement or parity claims.
"""
from __future__ import annotations
import argparse
import asyncio
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import secrets
import sys
import psycopg
from psycopg import sql
from neutron.orm import AsyncDatabase, Database, ColumnSpec, JsonDocument, Mutation, OrmError, Table, insert, select_row, update
import neutron.orm.client as client_module
import neutron.orm.core as core_module
import neutron.orm.endpoint as endpoint_module
from neutron.orm.endpoint import NUCLEUS_CANDIDATE_PROFILE


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def normalized(row: dict[str, object]) -> dict[str, object]:
    normalized_row={key: value.parsed() if isinstance(value, JsonDocument) else value.astimezone(dt.timezone.utc).isoformat() if isinstance(value, dt.datetime) else value for key, value in row.items()}
    normalized_row['_data_is_sql_null']=row['data'] is None
    return normalized_row


def model(schema: str, name: str) -> Table:
    return Table(name, {'id':ColumnSpec(int,'int8'),'active':ColumnSpec(bool,'bool'),
        'title':ColumnSpec(str,'text'),'data':ColumnSpec(JsonDocument,'jsonb',nullable=True),
        'stamp':ColumnSpec(dt.datetime,'timestamptz')},schema=schema)


STAMP=dt.datetime(2026,1,2,3,4,5,123456,tzinfo=dt.timezone.utc)
VALUES={'id':1,'active':False,'title':'','data':None,'stamp':STAMP}


class RollbackMarker(Exception): pass


def sync_facts(url: str, profile: str, table: Table) -> list[dict[str,object]]:
    key=table.column('id',int)
    query=select_row(table).where(key.eq(1))
    with Database.connect(url,profile=profile) as db:
        facts=[normalized(db.one(insert(table,VALUES).returning_row()))]
        with db.transaction():
            db.execute(update(table,{'title':'outer','data':JsonDocument('null')},where=key.eq(1)))
            try:
                with db.savepoint():
                    db.execute(update(table,{'title':'inner'},where=key.eq(1)))
                    raise RollbackMarker()
            except RollbackMarker: pass
            facts.append(normalized(db.one(query)))
        try:
            with db.transaction():
                db.execute(update(table,{'title':'must-rollback'},where=key.eq(1)))
                raise RollbackMarker()
        except RollbackMarker: pass
        facts.append(normalized(db.one(query)))
        if profile==NUCLEUS_CANDIDATE_PROFILE:
            for operation in [Mutation('DELETE FROM '+table.sql,()), Mutation('SELECT pg_cancel_backend(1)',())]:
                try: db.execute(operation)
                except OrmError: pass
                else: raise AssertionError('unsupported raw operation admitted')
            facts.append(normalized(db.one(query)))
        return facts


async def async_facts(url: str, profile: str, table: Table) -> list[dict[str,object]]:
    key=table.column('id',int)
    query=select_row(table).where(key.eq(1))
    async with await AsyncDatabase.connect(url,profile=profile) as db:
        facts=[normalized(await db.one(insert(table,VALUES).returning_row()))]
        async with db.transaction():
            await db.execute(update(table,{'title':'outer','data':JsonDocument('null')},where=key.eq(1)))
            try:
                async with db.savepoint():
                    await db.execute(update(table,{'title':'inner'},where=key.eq(1)))
                    raise RollbackMarker()
            except RollbackMarker: pass
            facts.append(normalized(await db.one(query)))
        try:
            async with db.transaction():
                await db.execute(update(table,{'title':'must-rollback'},where=key.eq(1)))
                raise RollbackMarker()
        except RollbackMarker: pass
        facts.append(normalized(await db.one(query)))
        if profile==NUCLEUS_CANDIDATE_PROFILE:
            try: await db.execute(Mutation('DELETE FROM '+table.sql,()))
            except OrmError: pass
            else: raise AssertionError('unsupported raw async operation admitted')
            facts.append(normalized(await db.one(query)))
        return facts


def main() -> None:
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--postgres-url-env',required=True)
    parser.add_argument('--nucleus-url-env',required=True)
    parser.add_argument('--package-root',type=Path,required=True)
    parser.add_argument('--binary-file',type=Path,required=True)
    parser.add_argument('--binary-sha256',required=True)
    parser.add_argument('--report',type=Path,required=True)
    args=parser.parse_args()
    root=args.package_root.resolve()
    package={}
    for module in (client_module,core_module,endpoint_module):
        path=Path(module.__file__).resolve()
        if not path.is_relative_to(root): parser.error('consumer module is outside required fresh package root')
        package[str(path.relative_to(root))]=digest(path)
    if digest(args.binary_file)!=args.binary_sha256: parser.error('Nucleus binary does not match required coordinator SHA256')
    report={'status':'fail','package_enabled':False,'profile':NUCLEUS_CANDIDATE_PROFILE,
        'scope':'finite Python admission/CRUD/rollback facts only','probeSha256':digest(Path(__file__)),
        'pythonSha256':digest(Path(sys.executable).resolve()),'packageFiles':package,
        'psycopgVersion':psycopg.__version__,'binarySha256':args.binary_sha256,
        'binaryAttestation':'coordinator-provided executed binary; endpoint report alone is not attestation','facts':{}}
    schema='np01_'+secrets.token_hex(6)
    connections=[]
    failure=None
    try:
        outputs={}
        for engine,env,profile in [('postgres',args.postgres_url_env,'postgres-direct'),('nucleus',args.nucleus_url_env,NUCLEUS_CANDIDATE_PROFILE)]:
            url=os.environ[env]
            native=psycopg.connect(url,autocommit=True)
            connections.append(native)
            native.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
            for mode in ('sync','async'):
                native.execute(sql.SQL('CREATE TABLE {}.{} (id bigint PRIMARY KEY, active boolean NOT NULL, title text NOT NULL, data jsonb, stamp timestamptz NOT NULL)').format(sql.Identifier(schema),sql.Identifier(mode)))
                table=model(schema,mode)
                outputs[engine,mode]=sync_facts(url,profile,table) if mode=='sync' else asyncio.run(async_facts(url,profile,table))
            if engine=='nucleus':
                try:
                    db=Database.connect(url)
                except OrmError: pass
                else:
                    db.close();raise AssertionError('default PostgreSQL profile admitted Nucleus')
        for mode in ('sync','async'):
            pg,nuc=outputs['postgres',mode],outputs['nucleus',mode]
            assert pg==nuc[:len(pg)],'PostgreSQL finite checkpoints disagree'
            assert nuc[-1]==pg[-1],'unsupported operation changed committed rows'
            assert pg[0]['title']=='' and pg[0]['active'] is False and pg[0]['data'] is None and pg[0]['_data_is_sql_null'] is True
            assert pg[1]['title']=='outer' and pg[1]['data'] is None and pg[1]['_data_is_sql_null'] is False
            assert pg[2]==pg[1],'rollback did not restore committed state'
        report['facts']={'checkpointAgreement':True,'syncAsyncAgreement':outputs['postgres','sync']==outputs['postgres','async'],
            'unsupportedRawPreservesRows':True,'defaultProfileRefusesNucleus':True}
        assert report['facts']['syncAsyncAgreement']
        report['status']='pass'
    except BaseException as error:
        failure=error
        cause=error.__cause__ or error.__context__
        report['failure']={'class':type(error).__name__,'sqlstate':getattr(error,'sqlstate',None),
            'reason':str(error) if isinstance(error,AssertionError) else None,
            'cause':None if cause is None else {'class':type(cause).__name__,'sqlstate':getattr(cause,'sqlstate',None),
                'message':(str(cause).splitlines() or [''])[0][:300]}}
    finally:
        for native in reversed(connections):
            try: native.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(sql.Identifier(schema)))
            except BaseException as error:
                if getattr(error,'sqlstate',None)=='0A000':
                    # The engine has no DROP SCHEMA: remove the fixture tables and record the empty schema as residue.
                    try:
                        for mode in ('sync','async'): native.execute(sql.SQL('DROP TABLE IF EXISTS {}.{}').format(sql.Identifier(schema),sql.Identifier(mode)))
                        report.setdefault('cleanupResidue',[]).append('schema '+schema+' remains: the engine does not support DROP SCHEMA')
                        error=None
                    except BaseException as table_error: error=table_error
                if error is not None:
                    failure=error;report['status']='fail';report['cleanupFailure']={'class':type(error).__name__,'sqlstate':getattr(error,'sqlstate',None),'message':(str(error).splitlines() or [''])[0][:300]}
            finally: native.close()
        args.report.parent.mkdir(parents=True,exist_ok=True)
        args.report.write_text(json.dumps(report,indent=2)+'\n')
    if failure is not None: raise SystemExit(1)


if __name__=='__main__': main()
