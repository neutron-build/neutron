"""Live tests use a disposable provisioned database and independent SQL oracle."""
import asyncio
from contextlib import asynccontextmanager
from decimal import Decimal
import hashlib
import json
import os
import sys
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
import unittest
from unittest.mock import patch
from uuid import uuid4

import asyncpg
from neutron.nucleus.client import NucleusClient
from main import Worker


@unittest.skipUnless(os.environ.get('WORKER_TEST_CREDENTIALS') and os.environ.get('ADMIN_DATABASE_URL'),
                     'live disposable reference database required')
class LiveWorker(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        manifest = json.loads(Path(os.environ['WORKER_TEST_CREDENTIALS']).read_text())
        if not manifest['database'].startswith('v10_data_'):
            raise ValueError('disposable database required')
        roles = manifest['roles']
        self.role = next(name for name in roles if name.endswith('_worker_a'))
        self.url = roles[self.role]
        self.db = await NucleusClient.connect(roles[self.role], min_size=1, max_size=2)
        self.worker = Worker(self.db, 'tenant-a')
        await self.worker.admit(self.role)
        admin_url = urlunsplit(urlsplit(os.environ['ADMIN_DATABASE_URL'])._replace(path='/' + manifest['database']))
        self.oracle = await asyncpg.connect(admin_url)
        self.project, self.document = uuid4(), uuid4()
        await self.oracle.execute("INSERT INTO projects VALUES('tenant-a',$1,'worker fixture')", self.project)
        await self.oracle.execute('''INSERT INTO documents(tenant_id,id,project_id,content,amount,note,payload,version,created_at)
            VALUES('tenant-a',$1,$2,'hello world',123.000000000000000001,NULL,$3,9007199254740993,
            '2026-09-30T00:00:00.123456Z')''', self.document, self.project, b'\x00\xff')
        await self.oracle.execute("INSERT INTO processing_requests VALUES('tenant-a',$1,$2,$3)", str(uuid4()), self.document, '0'*64)
        await self.oracle.execute("INSERT INTO jobs(tenant_id,document_id) VALUES('tenant-a',$1)", self.document)

    async def asyncTearDown(self):
        for table in ('results', 'jobs', 'processing_requests'):
            await self.oracle.execute('DELETE FROM ' + table + ' WHERE tenant_id=$1 AND document_id=$2', 'tenant-a', self.document)
        await self.oracle.execute('DELETE FROM documents WHERE tenant_id=$1 AND id=$2', 'tenant-a', self.document)
        await self.oracle.execute('DELETE FROM projects WHERE tenant_id=$1 AND id=$2', 'tenant-a', self.project)
        await self.db.close()
        await self.oracle.close()

    async def test_native_values_and_single_winner(self):
        claims = await asyncio.gather(self.worker.claim(), self.worker.claim())
        claimed = [value for value in claims if value is not None]
        self.assertEqual(len(claimed), 1)
        claim, document = claimed[0]
        self.assertEqual(document.amount, Decimal('123.000000000000000001'))
        self.assertEqual(document.version, 9007199254740993)
        self.assertIsNone(document.note)
        self.assertEqual(document.payload, b'\x00\xff')
        self.assertEqual(document.created_at.microsecond, 123456)
        self.assertTrue(await self.worker.finish(claim, document))
        self.assertFalse(await self.worker.finish(claim, document))
        result = await self.oracle.fetchrow('SELECT content_digest,word_count FROM results WHERE document_id=$1', self.document)
        self.assertEqual(result['content_digest'], hashlib.sha256(b'hello world').hexdigest())
        self.assertEqual(result['word_count'], 2)
        job = await self.oracle.fetchrow('SELECT status,attempts,claim_token,lease_until FROM jobs WHERE document_id=$1', self.document)
        self.assertEqual(tuple(job.values()), ('done', 1, None, None))

    async def test_locked_row_is_skipped(self):
        tx = self.oracle.transaction()
        await tx.start()
        try:
            await self.oracle.execute('SELECT 1 FROM jobs WHERE document_id=$1 FOR UPDATE', self.document)
            self.assertIsNone(await asyncio.wait_for(self.worker.claim(), timeout=1))
        finally:
            await tx.rollback()

    async def test_reclaim_replaces_token_and_rejects_stale_finish(self):
        old = await self.worker.claim()
        await self.oracle.execute("UPDATE jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE document_id=$1", self.document)
        new = await self.worker.claim()
        self.assertNotEqual(old[0].claim_token, new[0].claim_token)
        self.assertFalse(await self.worker.finish(*old))
        self.assertEqual(await self.oracle.fetchval('SELECT count(*) FROM results WHERE document_id=$1', self.document), 0)
        self.assertTrue(await self.worker.finish(*new))

    async def test_expired_final_attempt_is_terminal(self):
        await self.oracle.execute("UPDATE jobs SET status='processing',attempts=3,claim_token=$2,lease_until=clock_timestamp()-interval '1 second' WHERE document_id=$1", self.document, uuid4())
        self.assertIsNone(await self.worker.claim())
        row = await self.oracle.fetchrow('SELECT status,claim_token,lease_until,failure_code FROM jobs WHERE document_id=$1', self.document)
        self.assertEqual(tuple(row.values()), ('failed', None, None, 'attempts_exhausted'))

    async def test_expiry_after_result_rolls_back_result_and_ack(self):
        self.worker.lease_ms = 300
        claimed = await self.worker.claim()
        reached, resume = asyncio.Event(), asyncio.Event()
        actual_transaction = self.db.transaction

        class GatedSQL:
            def __init__(self, sql): self.sql = sql
            async def query(self, *args): return await self.sql.query(*args)
            async def query_one(self, *args): return await self.sql.query_one(*args)
            async def execute(self, sql, *args):
                result = await self.sql.execute(sql, *args)
                if 'INSERT INTO public.results' in sql:
                    reached.set()
                    await resume.wait()
                return result

        @asynccontextmanager
        async def gated_transaction():
            async with actual_transaction() as tx:
                tx.sql = GatedSQL(tx.sql)
                yield tx

        with patch.object(self.db, 'transaction', gated_transaction):
            finishing = asyncio.create_task(self.worker.finish(*claimed))
            await asyncio.wait_for(reached.wait(), 2)
            try:
                async def observe_expiry():
                    while await self.oracle.fetchval('SELECT clock_timestamp()<$1::timestamptz', claimed[0].lease_until):
                        await asyncio.sleep(0.01)
                await asyncio.wait_for(observe_expiry(), 2)
            finally:
                resume.set()
            self.assertFalse(await finishing)
        self.assertEqual(await self.oracle.fetchval('SELECT count(*) FROM results WHERE document_id=$1', self.document), 0)
        self.assertEqual(await self.oracle.fetchval('SELECT status FROM jobs WHERE document_id=$1', self.document), 'processing')

    async def test_cancellation_during_finish_rolls_back_and_pool_reuses(self):
        claimed = await self.worker.claim()
        reached = asyncio.Event()
        actual_transaction = self.db.transaction
        class GatedSQL:
            def __init__(self, sql): self.sql = sql
            async def query(self, *args): return await self.sql.query(*args)
            async def query_one(self, *args): return await self.sql.query_one(*args)
            async def execute(self, sql, *args):
                result = await self.sql.execute(sql, *args)
                if 'INSERT INTO public.results' in sql:
                    reached.set()
                    await asyncio.Event().wait()
                return result
        @asynccontextmanager
        async def gated_transaction():
            async with actual_transaction() as tx:
                tx.sql = GatedSQL(tx.sql)
                yield tx
        with patch.object(self.db, 'transaction', gated_transaction):
            finishing = asyncio.create_task(self.worker.finish(*claimed))
            await asyncio.wait_for(reached.wait(), 2)
            finishing.cancel()
            with self.assertRaises(asyncio.CancelledError): await finishing
        self.assertEqual(await self.oracle.fetchval('SELECT count(*) FROM results WHERE document_id=$1', self.document), 0)
        self.assertTrue(await asyncio.wait_for(self.worker.finish(*claimed), 2))

    async def test_expiry_during_row_lock_wait_does_not_insert_result(self):
        self.worker.lease_ms = 500
        claimed = await self.worker.claim()
        blocker = self.oracle.transaction()
        await blocker.start()
        await self.oracle.execute('SELECT 1 FROM public.jobs WHERE document_id=$1 FOR UPDATE', self.document)
        self.assertTrue(await self.oracle.fetchval('SELECT pg_catalog.clock_timestamp()<$1::timestamptz', claimed[0].lease_until))
        actual_transaction = self.db.transaction
        insert_attempts = []
        class TracedSQL:
            def __init__(self, sql): self.sql = sql
            async def query(self, *args): return await self.sql.query(*args)
            async def query_one(self, *args): return await self.sql.query_one(*args)
            async def execute(self, sql, *args):
                if 'INSERT INTO public.results' in sql: insert_attempts.append(sql)
                return await self.sql.execute(sql, *args)
        @asynccontextmanager
        async def traced_transaction():
            async with actual_transaction() as tx:
                tx.sql = TracedSQL(tx.sql)
                yield tx
        with patch.object(self.db, 'transaction', traced_transaction):
            finishing = asyncio.create_task(self.worker.finish(*claimed))
            try:
                async def observe_wait():
                    while True:
                        await self.oracle.execute('SELECT pg_catalog.pg_stat_clear_snapshot()')
                        waiting = await self.oracle.fetchval('''SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_stat_activity a
                            WHERE a.datname=pg_catalog.current_database()
                            AND $1=ANY(pg_catalog.pg_blocking_pids(a.pid)))''', self.oracle.get_server_pid())
                        if waiting: return
                        await asyncio.sleep(0.01)
                await asyncio.wait_for(observe_wait(), 2)
                async def observe_expiry():
                    while await self.oracle.fetchval('SELECT pg_catalog.clock_timestamp()<$1::timestamptz', claimed[0].lease_until):
                        await asyncio.sleep(0.01)
                await asyncio.wait_for(observe_expiry(), 2)
            finally:
                await blocker.rollback()
            self.assertFalse(await asyncio.wait_for(finishing, 2))
        self.assertEqual(insert_attempts, [])
        self.assertEqual(await self.oracle.fetchval('SELECT count(*) FROM results WHERE document_id=$1', self.document), 0)

    async def test_process_death_after_claim_reclaims_without_duplicate_result(self):
        child_code = """
import asyncio,json,os
from main import Worker
from neutron.nucleus.client import NucleusClient
async def run():
 db=await NucleusClient.connect(os.environ['WORKER_DATABASE_URL'],min_size=1,max_size=1)
 worker=Worker(db,'tenant-a',300)
 await worker.admit()
 claimed=await worker.claim()
 print(json.dumps({'token':str(claimed[0].claim_token),'expiry':claimed[0].lease_until.isoformat()}),flush=True)
 await asyncio.Event().wait()
asyncio.run(run())
"""
        process = await asyncio.create_subprocess_exec(sys.executable, '-c', child_code,
            env=dict(os.environ, WORKER_DATABASE_URL=self.url),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        try:
            line = await asyncio.wait_for(process.stdout.readline(), 3)
            self.assertTrue(line, 'child did not reach committed claim barrier')
            claim = json.loads(line)
            self.assertEqual(await self.oracle.fetchval('SELECT claim_token::text FROM jobs WHERE document_id=$1', self.document), claim['token'])
            process.kill()
            await asyncio.wait_for(process.wait(), 3)
            self.assertEqual(await self.oracle.fetchval('SELECT count(*) FROM results WHERE document_id=$1', self.document), 0)
            async def observe_expiry():
                while await self.oracle.fetchval('SELECT lease_until>clock_timestamp() FROM jobs WHERE document_id=$1', self.document):
                    await asyncio.sleep(0.01)
            await asyncio.wait_for(observe_expiry(), 3)
            claimed = await self.worker.claim()
            self.assertNotEqual(str(claimed[0].claim_token), claim['token'])
            self.assertTrue(await self.worker.finish(*claimed))
            self.assertEqual(await self.oracle.fetchval('SELECT count(*) FROM results WHERE document_id=$1', self.document), 1)
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()

    async def test_schema_and_identity_refusal(self):
        with self.assertRaises(ValueError): await self.worker.admit('wrong_configured_role')
        with self.assertRaises(ValueError): await Worker(self.db, 'tenant-b').admit()
        await self.oracle.execute('UPDATE app_schema_revision SET revision=2')
        try:
            with self.assertRaises(ValueError): await self.worker.admit()
        finally:
            await self.oracle.execute('UPDATE app_schema_revision SET revision=1')
        self.assertEqual(await self.oracle.fetchval('SELECT attempts FROM jobs WHERE document_id=$1', self.document), 0)


if __name__ == '__main__':
    unittest.main()
