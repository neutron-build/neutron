#!/usr/bin/env python3
"""Bounded local write/startup/RSS/load/adoption experiment; private raw evidence."""
from __future__ import annotations
import argparse, asyncio, hashlib, json, os, resource, secrets, stat, subprocess, sys, time
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

# Baseline before importing all provider libraries. All providers share this harness's
# import footprint; it is not an isolated minimal-package memory comparison.
BASELINE_RSS = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
import run
from providers import PROVIDERS, RollbackProbe
from extended_providers import workflow


def rss():
    # Darwin ru_maxrss bytes; Linux KiB. This is process high-water RSS, not heap.
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (1 if sys.platform == 'darwin' else 1024)


def current_rss():
    return int(subprocess.check_output(['ps', '-o', 'rss=', '-p', str(os.getpid())], text=True).strip()) * 1024


def row(id):
    return dict(tenant='a', id=id, project_id=3, version=0, amount=run.Decimal(run.DECIMAL), note=None, payload=bytes(range(256)))


def validate(result, input_row):
    changed, value, removed = result
    run.same((changed, removed), (1, 1))
    expected = {**input_row, 'version': 1, 'note': 'updated'}
    run.same(run.normalize(value), run.normalize(expected))



class DocumentService:
    """Small tenant-bound application use cases; same contract for all providers."""
    def __init__(self, provider, tenant):
        self.provider, self.tenant = provider, tenant

    async def create(self, id):
        value = {**row(id), 'tenant': self.tenant}
        run.same(await self.provider.create(value), 1)
        return await self.provider.point(self.tenant,id)

    async def edit(self, id, expected_version, note):
        return await self.provider.cas(self.tenant,id,expected_version,note)

    async def list(self, after):
        return await self.provider.page(self.tenant,after)

    async def detail(self, project):
        return await self.provider.relation(self.tenant,project)

    async def abandon(self, id):
        try:
            await self.provider.rolled_back_create({**row(id),'tenant':self.tenant})
        except RollbackProbe:
            return
        raise AssertionError('abandon committed')


async def adoption(provider, oracle):
    before = await run.snapshot(oracle)
    a, b = DocumentService(provider,'a'), DocumentService(provider,'b')
    try:
        run.same(run.normalize(await a.create(95000))['note'],None)
        await b.create(95000)
        run.same(await a.edit(95000,0,'A edit'),1)
        run.same(await a.edit(95000,0,'stale'),0)
        run.same(run.normalize(await provider.point('b',95000))['note'],None)
        run.same([run.normalize(r) for r in await a.list(94000)],await run.oracle_rows(oracle,"WHERE tenant='a' AND id>94000 ORDER BY id LIMIT 20"))
        detail = run.normalized_relation(await a.detail(3))
        run.same(detail['documents'],await run.oracle_rows(oracle,"WHERE tenant='a' AND project_id=3 ORDER BY id LIMIT 20"))
        await a.abandon(95001)
        run.same(await run.oracle_rows(oracle,"WHERE id=95001"),[])
        expected=(await run.oracle_rows(oracle,"WHERE tenant='a' AND id=95000"))[0]
        run.same(expected['version'],'1'); run.same(expected['note'],'A edit')
    finally:
        await provider.remove('a',95000)
        await provider.remove('b',95000)
    run.same(await run.snapshot(oracle),before)

async def child(args):
    config = json.loads(args.child_file.read_text())
    start = time.perf_counter_ns()
    provider = await PROVIDERS[config['provider']].open(config['url'])
    opened = time.perf_counter_ns()
    try:
        value = await provider.point('a',17)
        finished = time.perf_counter_ns()
        run.same(run.normalize(value), config['expected'])
        print(json.dumps({'connect_ns':opened-start,'first_read_ns':finished-opened,'baseline_peak_rss_bytes':BASELINE_RSS*(1 if sys.platform=='darwin' else 1024),'current_rss_bytes':current_rss(),'peak_rss_bytes':rss()}))
    finally:
        await provider.close()


async def execute(args):
    import asyncpg
    run.disk_guard(args.output.parent)
    if stat.S_IMODE(args.admin_file.stat().st_mode) & 0o077:
        raise RuntimeError('private admin file required')
    args.output.mkdir(mode=0o700, exist_ok=False)
    run.write_json(args.output/'source.json',run.source_identity())
    admin = json.loads(args.admin_file.read_text())['postgres_admin_url']
    parts = urlsplit(admin)
    database = 'neutron_extended_py_'+secrets.token_hex(8)
    owner = await asyncpg.connect(admin)
    oracle = None
    active = []
    created = False
    failure = None
    try:
        server = await owner.fetchrow("SELECT version() AS version,current_setting('server_version_num') AS number")
        if not 170000 <= int(server['number']) < 180000: raise RuntimeError('PostgreSQL17 required')
        await owner.execute(f'CREATE DATABASE "{database}"')
        created = True
        url = urlunsplit((parts.scheme,parts.netloc,'/'+database,'',''))
        oracle = await asyncpg.connect(url)
        await oracle.execute(run.DDL)
        correctness = []
        for factory in PROVIDERS:
            p = await factory.open(url); active.append(p)
            correctness.append(await run.correctness(p,oracle))
            await run.seed_measurement(oracle)
            before = await run.snapshot(oracle)
            validate(await workflow(p,row(90000)),row(90000))
            run.same(await run.snapshot(oracle),before)
            try: await workflow(p,row(90000),rollback=True)
            except RollbackProbe: pass
            else: raise AssertionError('rollback did not throw')
            run.same(await run.snapshot(oracle),before)
            await adoption(p,oracle)
            # Adoption: tenant-safe point and keyset, parent/detail, guarded update,
            # transactional CRUD, explicit rollback and native database oracle.
            run.same(run.normalize(await p.point('a',17)),(await run.oracle_rows(oracle,"WHERE tenant='a' AND id=17"))[0])
            run.same([run.normalize(r) for r in await p.page('a',20)],await run.oracle_rows(oracle,"WHERE tenant='a' AND id>20 ORDER BY id LIMIT 20"))
            correctness[-1]['extended_workflow']='PASS: transaction create/CAS/read/delete, commit/rollback and exact immutable fixture'
            await p.close(); active.remove(p)
        run.write_json(args.output/'correctness.json',correctness)
        measurements = []
        if not args.correctness_only:
            expected = (await run.oracle_rows(oracle,"WHERE tenant='a' AND id=17"))[0]
            for trial, order in enumerate(run.WILLIAMS_ORDER):
                for index in order:
                    run.disk_guard(args.output)
                    # Child secret configuration exists only during subprocess and is removed.
                    child_file = args.output/('child-'+secrets.token_hex(8)+'.local.json')
                    run.write_json(child_file,{'provider':index,'url':url,'expected':expected})
                    start = time.perf_counter_ns()
                    try:
                        proc = await asyncio.create_subprocess_exec(sys.executable,str(Path(__file__).resolve()),'--child-file',str(child_file),stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.PIPE)
                        stdout, stderr = await asyncio.wait_for(proc.communicate(),timeout=30)
                        if proc.returncode: raise RuntimeError('startup child failed')
                        cold = json.loads(stdout)
                        cold['process_wall_ns']=time.perf_counter_ns()-start
                    finally: child_file.unlink(missing_ok=True)
                    p = await PROVIDERS[index].open(url); active.append(p)
                    before = await run.snapshot(oracle)
                    samples = []
                    for n in range(args.samples):
                        value = row(90000)
                        start = time.perf_counter_ns(); result = await workflow(p,value); elapsed=time.perf_counter_ns()-start
                        validate(result,value)
                        run.same(await run.oracle_rows(oracle,"WHERE tenant='a' AND id=90000"),[])
                        samples.append(elapsed)
                    # Closed-loop c4 workload, 3 reads then one transactional CRUD.
                    # All workers own distinct write IDs. Checks outside latency timer.
                    sustained = []
                    started=time.perf_counter_ns(); deadline=time.monotonic()+args.seconds
                    async def worker(worker_id):
                        count=0
                        while time.monotonic()<deadline:
                            write=count%4==3
                            value=row(91000+worker_id)
                            t=time.perf_counter_ns()
                            result=await workflow(p,value) if write else await p.point('a',17)
                            elapsed=time.perf_counter_ns()-t
                            if write:
                                validate(result,value)
                                run.same(await run.oracle_rows(oracle,"WHERE tenant='a' AND id=$1",value['id']),[])
                            else: run.same(run.normalize(result),expected)
                            sustained.append({'offset_ns':time.perf_counter_ns()-started,'worker':worker_id,'sequence':count,'operation':'transaction-crud' if write else 'point','elapsed_ns':elapsed})
                            count+=1
                    rss_samples=[]
                    async def sample_memory():
                        while time.monotonic()<deadline:
                            rss_samples.append({'offset_ns':time.perf_counter_ns()-started,'current_rss_bytes':current_rss(),'os_high_water_rss_bytes':rss()})
                            await asyncio.sleep(1)
                    await asyncio.gather(*(worker(i) for i in range(4)),sample_memory())
                    duration=time.perf_counter_ns()-started
                    run.same(await run.snapshot(oracle),before)
                    phase={'trial':trial,'position':order.index(index),'provider':p.name,'startup':cold,'writes_ns':samples,'sustained':sustained,'sustained_wall_ns':duration,'rss_samples':rss_samples,'rss_parent_current_bytes':current_rss(),'rss_parent_peak_bytes':rss(),'snapshot_sha256':hashlib.sha256(json.dumps(before,sort_keys=True).encode()).hexdigest()}
                    phase_path=args.output/f'phase-{trial}-{index}.json'
                    run.write_json(phase_path,phase)
                    measurements.append({'trial':trial,'position':order.index(index),'provider':p.name,'file':phase_path.name,'writes':len(samples),'sustained_calls':len(sustained),'sustained_wall_ns':duration})
                    del phase,samples,sustained,rss_samples
                    await p.close(); active.remove(p)
            run.write_json(args.output/'raw.json',measurements)
        run.write_json(args.output/'environment.json',{'server':dict(server),'python':sys.version,'platform':sys.platform,'pool_max_each':4,'samples':args.samples,'sustained_seconds_each':args.seconds,'trials':4,'scope':'parent current/high-water RSS is shared-process with allocator/import history; raw phases serialized and discarded but no provider memory ranking; memory sampler uses ps every1s and adds overhead; consumer throughput includes validation/oracle/dispatch; API latencies exclude validation; warm OS/server fresh child startup; common all-provider imports; process RSS, no heap attribution; closed loop load including oracle overhead; sequential provider measurement'})
    except BaseException as e: failure=e
    finally:
        errors=[]
        for p in active:
            try: await p.close()
            except Exception: errors.append('provider-close')
        if oracle: await oracle.close()
        if created:
            try:
                await owner.execute('SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1 AND pid<>pg_backend_pid()',database)
                await owner.execute(f'DROP DATABASE "{database}"')
                run.same(await owner.fetchval('SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)',database),False)
            except Exception: errors.append('database-cleanup')
        await owner.close()
        run.write_json(args.output/'cleanup.json',{'database':database,'created':created,'removed':created and not errors,'errors':errors})
        if errors and failure is None: failure=RuntimeError('cleanup failed')
    if failure:
        run.write_json(args.output/'failure.json',{'type':type(failure).__name__})
        raise failure
    run.disk_guard(args.output)
    run.write_json(args.output/'result.json',{'status':'PASS','mode':'correctness-only' if args.correctness_only else 'extended','phases':len(measurements),'errors':0})
    print('PASS: Python extended evaluation and cleanup')


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--admin-file',type=Path)
    parser.add_argument('--output',type=Path)
    parser.add_argument('--samples',type=int,default=100)
    parser.add_argument('--seconds',type=float,default=30)
    parser.add_argument('--correctness-only',action='store_true')
    parser.add_argument('--child-file',type=Path)
    args=parser.parse_args()
    if not args.child_file and (not args.admin_file or not args.output or args.samples<10 or not 1<=args.seconds<=30): parser.error('admin/output required; samples>=10; seconds1..30')
    try: asyncio.run(child(args) if args.child_file else execute(args))
    except BaseException as e:
        print('FAIL: '+type(e).__name__+'; private evidence retained',file=sys.stderr)
        raise SystemExit(1)

if __name__=='__main__': main()
