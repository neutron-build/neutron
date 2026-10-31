#!/usr/bin/env python3
"""Live schema/permission checks on a freshly provisioned disposable database."""
import argparse
import asyncio
import json
from pathlib import Path
import asyncpg

async def checks(out):
 manifest=json.loads(out.read_text())
 if not manifest['database'].startswith('v10_data_'): raise ValueError('disposable reference database required')
 urls=manifest['roles']; roles={k.rsplit('_',2)[-2]+'_'+k[-1]:v for k,v in urls.items()}
 a=await asyncpg.connect(roles['api_a']);other=await asyncpg.connect(roles['api_b']);studio=await asyncpg.connect(roles['studio_a']);worker=await asyncpg.connect(roles['worker_a'])
 report={}
 try:
  for db in (a,other,studio,worker):
   identity=await db.fetchrow('SELECT current_user AS role, session_user AS login')
   flags=await db.fetchrow('SELECT rolsuper,rolbypassrls,rolcreaterole FROM pg_roles WHERE rolname=current_user')
   assert identity['role']==identity['login'] and not any(flags.values())
  pid='00000000-0000-0000-0000-000000000001';did='00000000-0000-0000-0000-000000000002'
  for db,tenant in ((a,'tenant-a'),(other,'tenant-b')):
   await db.execute("INSERT INTO projects VALUES($1,$2,'project')",tenant,pid)
   await db.execute("INSERT INTO documents(tenant_id,id,project_id,content,amount,note,payload,version) VALUES($1,$2,$3,'hello world',123.000000000000000001,NULL,$4,9007199254740993)",tenant,did,pid,b'\x00\xff')
   await db.execute("INSERT INTO processing_requests VALUES($1,'key',$2,$3)",tenant,did,'0'*64)
   await db.execute("INSERT INTO jobs(tenant_id,document_id) VALUES($1,$2)",tenant,did)
  assert await a.fetchval('SELECT count(*) FROM documents')==1
  assert await other.fetchval('SELECT count(*) FROM documents')==1
  assert await studio.fetchval('SELECT count(*) FROM documents')==1
  assert await worker.fetchval('SELECT count(*) FROM documents')==1
  row=await a.fetchrow('SELECT amount::text AS amount,version::text AS version,note,payload FROM documents')
  assert row['amount']=='123.000000000000000001' and row['version']=='9007199254740993' and row['note'] is None and row['payload']==b'\x00\xff'
  for db,sql in ((a,"INSERT INTO projects VALUES('tenant-b','00000000-0000-0000-0000-000000000003','leak')"),(studio,"UPDATE documents SET note='mutated'"),(worker,"UPDATE documents SET note='mutated'"),(a,'SET ROLE '+list(urls)[-1])):
   try: await db.execute(sql)
   except asyncpg.PostgresError:pass
   else:raise AssertionError('forbidden role operation succeeded')
  assert await a.fetchval("UPDATE documents SET note='cross' WHERE tenant_id='tenant-b' RETURNING id") is None
  for amount in ('NaN','Infinity','-Infinity'):
   try:await a.execute('UPDATE documents SET amount=$1::numeric',amount)
   except (asyncpg.CheckViolationError, asyncpg.NumericValueOutOfRangeError):pass
   else:raise AssertionError('nonfinite accepted')
  report={'checks':'role identity/flags, two tenants same IDs, exact values, cross-tenant refusal, Studio readonly, worker least privilege, SET ROLE denied, nonfinite amounts refused','outcome':'PASS'}
 finally:
  for db in (a,other,studio,worker):await db.close()
 print(json.dumps(report))

if __name__ == '__main__':
 parser=argparse.ArgumentParser(description=__doc__)
 parser.add_argument('--credentials', type=Path, required=True)
 try:
  asyncio.run(checks(parser.parse_args().credentials))
 except Exception as error:
  print('Schema check failed: '+type(error).__name__)
  raise SystemExit(1)
