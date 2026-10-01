#!/usr/bin/env python3
"""PostgreSQL reference worker: native Neutron SQL, fenced leases, pure effects."""
from __future__ import annotations
import argparse
import asyncio
from datetime import datetime
from decimal import Decimal
import hashlib
import json
import os
import signal
from uuid import UUID, uuid4
from urllib.parse import unquote, urlsplit

from pydantic import BaseModel
from neutron.nucleus.client import NucleusClient


class Identity(BaseModel):
    role: str
    login: str
    superuser: bool
    bypass: bool
    owner: bool
    createrole: bool
    createdb: bool
    memberships: bool
    rls_valid: bool
    revision: int
    server_major: int


class Claim(BaseModel):
    document_id: UUID
    claim_token: UUID
    attempts: int
    lease_until: datetime


class Document(BaseModel):
    content: str
    amount: Decimal
    version: int
    note: str | None
    payload: bytes
    created_at: datetime


class LockedJob(BaseModel):
    claim_token: UUID | None
    status: str


class FinishState(LockedJob):
    unexpired: bool


class LeaseLost(Exception):
    pass


class Worker:
    def __init__(self, db: NucleusClient, tenant: str, lease_ms: int = 30000):
        if tenant not in ('tenant-a', 'tenant-b') or not 1 <= lease_ms <= 30000:
            raise ValueError('bounded reference tenant and lease required')
        self.db, self.tenant, self.lease_ms = db, tenant, lease_ms

    async def admit(self, expected_role: str | None = None):
        if self.db.features.is_nucleus:
            raise ValueError('this reference requires PostgreSQL 17')
        identity = await self.db.sql.query_one(Identity, '''
            SELECT current_user AS role, session_user AS login,
              r.rolsuper AS superuser, r.rolbypassrls AS bypass, r.rolcreaterole AS createrole, r.rolcreatedb AS createdb,
              EXISTS(SELECT 1 FROM pg_catalog.pg_auth_members WHERE member=r.oid) AS memberships,
              (SELECT count(*)=5 AND bool_and(c.relrowsecurity AND c.relforcerowsecurity AND c.relowner<>r.oid)
                FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
                WHERE n.nspname='public' AND c.relkind='r'
                  AND c.relname IN ('projects','documents','processing_requests','jobs','results')) AS rls_valid,
              EXISTS(SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n
                ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relname='jobs'
                AND c.relowner=r.oid) AS owner,
              (SELECT revision FROM public.app_schema_revision WHERE singleton) AS revision,
              pg_catalog.current_setting('server_version_num')::integer / 10000 AS server_major
            FROM pg_catalog.pg_roles r WHERE r.rolname=current_user''')
        suffix = '_worker_' + self.tenant[-1]
        if (identity.role != identity.login or not identity.role.endswith(suffix)
                or identity.superuser or identity.bypass or identity.owner or identity.createrole or identity.createdb
                or identity.memberships or not identity.rls_valid
                or (expected_role is not None and identity.role != expected_role)
                or identity.revision != 1 or identity.server_major != 17):
            raise ValueError('runtime role or schema/profile is incompatible')

    async def claim(self) -> tuple[Claim, Document] | None:
        token = uuid4()
        async with self.db.transaction() as tx:
            await tx.sql.execute("SET LOCAL statement_timeout = '5s'")
            # Retire expired final attempts without blocking another worker's row.
            await tx.sql.execute('''
                WITH exhausted AS (
                  SELECT tenant_id,document_id FROM public.jobs
                  WHERE tenant_id=$1 AND status='processing' AND attempts=3
                    AND lease_until <= pg_catalog.clock_timestamp()
                  ORDER BY document_id LIMIT 1 FOR UPDATE SKIP LOCKED
                ) UPDATE public.jobs j SET status='failed',claim_token=NULL,
                    lease_until=NULL,failure_code='attempts_exhausted'
                  FROM exhausted e WHERE j.tenant_id=e.tenant_id
                    AND j.document_id=e.document_id''', self.tenant)
            rows = await tx.sql.query(Claim, '''
                WITH candidate AS (
                  SELECT tenant_id,document_id FROM public.jobs
                  WHERE tenant_id=$1 AND attempts<3
                    AND (status='pending' OR (status='processing' AND lease_until <= pg_catalog.clock_timestamp()))
                  ORDER BY document_id LIMIT 1 FOR UPDATE SKIP LOCKED
                ) UPDATE public.jobs j SET status='processing',attempts=j.attempts+1,
                    claim_token=$2,lease_until=pg_catalog.clock_timestamp()+($3::integer*interval '1 millisecond'),
                    failure_code=NULL
                  FROM candidate c WHERE j.tenant_id=c.tenant_id AND j.document_id=c.document_id
                  RETURNING j.document_id,j.claim_token,j.attempts,j.lease_until''',
                self.tenant, token, self.lease_ms)
            if not rows:
                return None
            claim = rows[0]
            document = await tx.sql.query_one(Document, '''
                SELECT content,amount,version,note,payload,created_at
                FROM public.documents WHERE tenant_id=$1 AND id=$2''',
                self.tenant, claim.document_id)
            return claim, document

    async def finish(self, claim: Claim, document: Document) -> bool:
        # Pure computation outside the transaction. No external side effect.
        digest = hashlib.sha256(document.content.encode('utf-8')).hexdigest()
        word_count = len(document.content.split())
        try:
            async with self.db.transaction() as tx:
                await tx.sql.execute("SET LOCAL statement_timeout = '5s'")
                rows = await tx.sql.query(LockedJob, '''
                    SELECT claim_token,status
                    FROM public.jobs WHERE tenant_id=$1 AND document_id=$2 FOR UPDATE''',
                    self.tenant, claim.document_id)
                if not rows or rows[0].claim_token != claim.claim_token or rows[0].status != 'processing':
                    return False
                # PostgreSQL may evaluate target expressions before a row-lock
                # wait. Evaluate wall-clock expiry in a new statement AFTER the
                # lock-only query has returned.
                state = await tx.sql.query_one(FinishState, '''
                    SELECT claim_token,status,COALESCE(lease_until > pg_catalog.clock_timestamp(), false) AS unexpired
                    FROM public.jobs WHERE tenant_id=$1 AND document_id=$2''',
                    self.tenant, claim.document_id)
                if not state.unexpired:
                    return False
                await tx.sql.execute('''
                    INSERT INTO public.results(tenant_id,document_id,content_digest,word_count)
                    VALUES($1,$2,$3,$4)
                    ON CONFLICT(tenant_id,document_id) DO UPDATE
                      SET content_digest=EXCLUDED.content_digest,word_count=EXCLUDED.word_count''',
                    self.tenant, claim.document_id, digest, word_count)
                acknowledged = await tx.sql.execute('''
                    UPDATE public.jobs SET status='done',claim_token=NULL,lease_until=NULL,failure_code=NULL
                    WHERE tenant_id=$1 AND document_id=$2 AND claim_token=$3
                      AND status='processing' AND lease_until > pg_catalog.clock_timestamp()''',
                    self.tenant, claim.document_id, claim.claim_token)
                if acknowledged != 1:
                    raise LeaseLost('lease expired before acknowledgement')
                return True
        except LeaseLost:
            return False

    async def once(self) -> str:
        claimed = await self.claim()
        if claimed is None:
            return 'idle'
        return 'done' if await self.finish(*claimed) else 'lease_lost'


async def run(args):
    url = os.environ['WORKER_DATABASE_URL']
    tenant = os.environ['WORKER_TENANT']
    # Pool and per-transaction statement budgets use the existing native APIs.
    db = await asyncio.wait_for(NucleusClient.connect(url, min_size=1, max_size=2), timeout=15)
    try:
        worker = Worker(db, tenant, args.lease_ms)
        await asyncio.wait_for(worker.admit(unquote(urlsplit(url).username or '')), timeout=5)
        if args.once:
            print(json.dumps({'tenant': tenant, 'outcome': await worker.once()}))
            return
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
        print(json.dumps({'tenant': tenant, 'ready': True}), flush=True)
        while not stop.is_set():
            outcome = await worker.once()
            if outcome != 'idle':
                print(json.dumps({'tenant': tenant, 'outcome': outcome}), flush=True)
            try:
                await asyncio.wait_for(stop.wait(), timeout=0.2 if outcome == 'idle' else 0.01)
            except TimeoutError:
                pass
    finally:
        await db.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--once', action='store_true')
    parser.add_argument('--lease-ms', type=int, default=30000,
                        help='1..30000; shorter leases are for controlled tests')
    try:
        asyncio.run(run(parser.parse_args()))
    except Exception as error:
        # An uncertain commit is not automatically retried. Reconnect/query before
        # recovery; an abandoned claim is eligible only after its database expiry.
        print(json.dumps({'failed': True, 'category': type(error).__name__}), flush=True)
        raise SystemExit(1)
