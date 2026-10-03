"""Installed native Python ORM scalar reader/writer; stdout is one protocol envelope."""
from __future__ import annotations
import argparse
import asyncio
import datetime as dt
from decimal import Decimal
import importlib.metadata
import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from protocol import PROTOCOL, redact, validate_request, verify_artifacts


def execute(request: dict, mode: str, prefix: Path, artifact_root: Path) -> dict:
    validate_request(request)
    if request['case_id'] != 'scalar-extremes' or request['action'] not in {'observe','insert'}:
        raise ValueError('unsupported Python adapter operation')
    if Path(sys.prefix).resolve() != prefix.resolve():
        raise ValueError('adapter requires the designated fresh installed environment')
    import neutron.orm as orm
    import psycopg
    for module in (orm, psycopg):
        if not Path(module.__file__).resolve().is_relative_to(prefix.resolve()):
            raise ValueError('adapter module resolved outside installed environment')
    manifest=json.loads((artifact_root / 'artifacts.json').read_text())
    hashes=verify_artifacts(manifest,artifact_root)
    if request.get('artifact_hashes') != hashes:
        raise ValueError('installed artifact bytes differ from runner identity')
    table=orm.Table('values_fixture',{
        'id':orm.ColumnSpec(int,'int4'),
        'big':orm.ColumnSpec(int,'int8',nullable=True),
        'precise':orm.ColumnSpec(Decimal,'numeric',nullable=True),
        'moment':orm.ColumnSpec(dt.datetime,'timestamptz',nullable=True),
        'sql_null':orm.ColumnSpec(str,'text',nullable=True),
        'document':orm.ColumnSpec(orm.JsonDocument,'jsonb',nullable=True),
    },schema=request['schema_scope'])
    query=orm.select_row(table)
    mutation=orm.insert(table,{'id':2,'big':-(2**63),
        'precise':Decimal('-98765432109876543210.000000001'),
        'moment':dt.datetime(2038,1,19,3,14,7,654321,tzinfo=dt.timezone.utc),
        'sql_null':None,'document':orm.JsonDocument('null')})
    url=os.environ.get('NEUTRON_TEST_DATABASE_URL')
    if not url: raise ValueError('live PostgreSQL URL required')
    if mode == 'sync':
        with orm.Database.connect(url) as db:
            if request['action']=='insert':
                if db.execute(mutation)!=1: raise ValueError('insert cardinality differs')
                rows=[]
            else: rows=db.all(query)
    elif mode == 'async':
        async def read():
            async with await orm.AsyncDatabase.connect(url) as db:
                if request['action']=='insert':
                    if await db.execute(mutation)!=1: raise ValueError('insert cardinality differs')
                    return []
                return await db.all(query)
        rows=asyncio.run(read())
    else: raise ValueError('unsupported execution mode')
    observed=[]
    # SELECT order is unspecified; expose the oracle's explicit id ordering.
    for row in sorted(rows,key=lambda item:item['id']):
        moment=row['moment']; document=row['document']
        if type(row['id']) is not int or type(row['big']) is not int or not isinstance(row['precise'],Decimal):
            raise ValueError('native numeric result types not preserved')
        if not isinstance(moment,dt.datetime) or moment.utcoffset() is None:
            raise ValueError('native aware timestamp not preserved')
        if not isinstance(document,orm.JsonDocument):
            raise ValueError('JSON document collapsed into SQL NULL or an untyped value')
        timestamp=moment.astimezone(dt.timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f+00')
        observed.append([str(row['id']),str(row['big']),str(row['precise']),timestamp,row['sql_null'] is None,document.text])
    return {'protocol':PROTOCOL,'case_id':request['case_id'],'profile':request['profile'],
            'schema_scope':request['schema_scope'],'status':'pass','rows':observed,
            'artifact_hashes':hashes,'mode':mode,'client_version':importlib.metadata.version('neutron-framework'),
            'driver_version':importlib.metadata.version('psycopg')}


def main() -> int:
    parser=argparse.ArgumentParser()
    parser.add_argument('--mode',choices=('sync','async'),required=True)
    parser.add_argument('--prefix',type=Path,required=True)
    parser.add_argument('--artifact-root',type=Path,required=True)
    args=parser.parse_args()
    try:
        print(json.dumps(execute(json.load(sys.stdin),args.mode,args.prefix,args.artifact_root)))
        return 0
    except BaseException as exc:
        # Native causes and subprocess stderr are never copied into public output.
        print(json.dumps({'status':'fail','diagnostics':redact(type(exc).__name__ + ': installed Python adapter failed',os.environ.get('NEUTRON_TEST_DATABASE_URL',''))}))
        return 1

if __name__ == '__main__':
    raise SystemExit(main())
