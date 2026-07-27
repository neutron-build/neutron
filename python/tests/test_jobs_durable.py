"""Durable-mode job queue: claim semantics, leases, restart recovery.

These are the guarantees N-002 found missing. They need a real PostgreSQL —
SKIP LOCKED and advisory locks have no in-memory equivalent to fake.

    docker compose up -d postgres
    NEUTRON_TEST_DATABASE_URL=postgresql://... pytest tests/test_jobs_durable.py
"""

from __future__ import annotations

import asyncio
import os

import asyncpg
import pytest
import pytest_asyncio

from neutron.jobs.queue import JobQueue, JobStatus

DATABASE_URL = os.environ.get("NEUTRON_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DATABASE_URL,
    reason="NEUTRON_TEST_DATABASE_URL is not set; durable queue tests need PostgreSQL",
)


class _Db:
    """Minimal stand-in for NucleusClient: the queue only needs a pool."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool


@pytest_asyncio.fixture
async def db():
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    async with pool.acquire() as conn:
        await conn.execute("DROP TABLE IF EXISTS _neutron_jobs")
    try:
        yield _Db(pool)
    finally:
        await pool.close()


async def _drain(queue: JobQueue, job_id: str, timeout: float = 5.0) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        job = await queue.fetch_job(job_id)
        if job and job.status in (JobStatus.COMPLETED, JobStatus.FAILED):
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"job {job_id} did not finish within {timeout}s")


class TestDurability:
    async def test_a_job_survives_the_process_that_enqueued_it(self, db):
        """The failure N-002 describes: enqueue, restart, work is gone."""
        producer = JobQueue(db=db)
        job_id = await producer.enqueue("greet", {"name": "ada"})

        # A different JobQueue instance stands in for a restarted process:
        # it shares no memory with the producer.
        consumer = JobQueue(db=db, poll_interval=0.05)
        seen: list[str] = []

        @consumer.handler("greet")
        async def _greet(payload: dict) -> dict:
            seen.append(payload["name"])
            return {"greeted": payload["name"]}

        await consumer.start_worker()
        try:
            await _drain(consumer, job_id)
        finally:
            await consumer.stop_worker()

        assert seen == ["ada"]
        job = await consumer.fetch_job(job_id)
        assert job.status == JobStatus.COMPLETED
        assert job.result == {"greeted": "ada"}

    async def test_pending_work_is_not_lost_when_no_worker_is_running(self, db):
        producer = JobQueue(db=db)
        job_id = await producer.enqueue("later", {})
        row = await db.pool.fetchrow(
            "SELECT status FROM _neutron_jobs WHERE id = $1", job_id
        )
        assert row["status"] == "pending"

    async def test_setup_failure_is_raised_not_swallowed(self):
        """A queue asked for durability must refuse rather than degrade."""

        class Broken:
            @property
            def pool(self):
                raise RuntimeError("database is down")

        queue = JobQueue(db=Broken())
        with pytest.raises(RuntimeError, match="database is down"):
            await queue.enqueue("anything", {})


class TestClaimSemantics:
    async def test_two_workers_never_run_the_same_job(self, db):
        """SKIP LOCKED: each job is delivered exactly once across workers."""
        job_count = 30
        producer = JobQueue(db=db)
        await producer._ensure_db()

        processed: list[int] = []
        by_worker: dict[str, int] = {}
        lock = asyncio.Lock()

        def build(name: str) -> JobQueue:
            q = JobQueue(db=db, poll_interval=0.01, worker_id=name)

            @q.handler("count")
            async def _count(payload: dict) -> None:
                # Hold each job briefly so claims genuinely overlap; without
                # this one worker can drain the table before the others poll,
                # and the test would pass without exercising SKIP LOCKED.
                await asyncio.sleep(0.02)
                async with lock:
                    processed.append(payload["n"])
                    by_worker[name] = by_worker.get(name, 0) + 1

            return q

        ids = [await producer.enqueue("count", {"n": i}) for i in range(job_count)]

        workers = [build(f"w{i}") for i in range(4)]
        for w in workers:
            await w.start_worker(concurrency=2)
        try:
            for job_id in ids:
                await _drain(workers[0], job_id, timeout=15.0)
        finally:
            for w in workers:
                await w.stop_worker()

        assert sorted(processed) == list(range(job_count))
        assert len(processed) == len(set(processed)), "a job ran more than once"
        assert len(by_worker) > 1, (
            f"only {list(by_worker)} claimed anything — concurrent claiming was "
            "never exercised"
        )

    async def test_attempts_increments_once_per_claim(self, db):
        queue = JobQueue(db=db, poll_interval=0.05)

        @queue.handler("ok")
        async def _ok(payload: dict) -> None:
            return None

        job_id = await queue.enqueue("ok", {})
        await queue.start_worker()
        try:
            await _drain(queue, job_id)
        finally:
            await queue.stop_worker()

        job = await queue.fetch_job(job_id)
        assert job.attempts == 1


class TestLeases:
    async def test_a_dead_workers_job_is_reclaimed(self, db):
        """No heartbeat table: an expired lease is what makes a hard kill safe."""
        victim = JobQueue(db=db, lease_ttl=0.5, poll_interval=0.02, worker_id="victim")
        started = asyncio.Event()

        @victim.handler("stall")
        async def _stall(payload: dict) -> None:
            started.set()
            await asyncio.sleep(3600)

        job_id = await victim.enqueue("stall", {})
        await victim.start_worker()
        await asyncio.wait_for(started.wait(), timeout=5.0)

        # Kill the worker without letting it finish or release the lease.
        for task in victim._workers:
            task.cancel()
        victim._workers.clear()
        victim._running = False

        rescuer = JobQueue(db=db, lease_ttl=30, poll_interval=0.02, worker_id="rescuer")
        rescued = asyncio.Event()

        @rescuer.handler("stall")
        async def _rescue(payload: dict) -> None:
            rescued.set()

        await rescuer.start_worker()
        try:
            await asyncio.wait_for(rescued.wait(), timeout=10.0)
            await _drain(rescuer, job_id)
        finally:
            await rescuer.stop_worker()

        job = await rescuer.fetch_job(job_id)
        assert job.status == JobStatus.COMPLETED
        assert job.attempts == 2, "the reclaim should count as a second attempt"

    async def test_a_heartbeat_keeps_a_slow_job(self, db):
        """A job slower than the lease must not be stolen from a live worker."""
        runs: list[int] = []
        queue = JobQueue(db=db, lease_ttl=0.3, poll_interval=0.02, worker_id="slow")

        @queue.handler("slow")
        async def _slow(payload: dict) -> None:
            runs.append(1)
            await asyncio.sleep(1.2)

        job_id = await queue.enqueue("slow", {})
        await queue.start_worker(concurrency=3)
        try:
            await _drain(queue, job_id, timeout=15.0)
        finally:
            await queue.stop_worker()

        assert len(runs) == 1, "the heartbeat did not hold the lease"


class TestRetry:
    async def test_a_failing_job_retries_then_fails_with_its_error(self, db):
        queue = JobQueue(db=db, retry_delay_base=0.01, poll_interval=0.02)
        calls: list[int] = []

        @queue.handler("boom")
        async def _boom(payload: dict) -> None:
            calls.append(1)
            raise ValueError("upstream unavailable")

        job_id = await queue.enqueue("boom", {}, max_retries=3)
        await queue.start_worker()
        try:
            await _drain(queue, job_id, timeout=15.0)
        finally:
            await queue.stop_worker()

        job = await queue.fetch_job(job_id)
        assert job.status == JobStatus.FAILED
        assert job.error == "upstream unavailable"
        assert job.attempts == 3
        assert len(calls) == 3

    async def test_a_job_with_no_handler_fails_loudly(self, db):
        queue = JobQueue(db=db, poll_interval=0.02)

        @queue.handler("known")
        async def _known(payload: dict) -> None:
            return None

        job_id = await queue.enqueue("unknown", {})
        await queue.start_worker()
        try:
            await _drain(queue, job_id)
        finally:
            await queue.stop_worker()

        job = await queue.fetch_job(job_id)
        assert job.status == JobStatus.FAILED
        assert "No handler registered" in job.error


class TestSchedulerElection:
    async def test_only_one_instance_holds_the_scheduler_lock(self, db):
        """Two replicas must not both fire every cron entry."""
        a = JobQueue(db=db)
        b = JobQueue(db=db)
        await a._ensure_db()

        assert await a._acquire_scheduler_lock() is True
        try:
            assert await b._acquire_scheduler_lock() is False
        finally:
            await a._release_scheduler_lock()

        assert await b._acquire_scheduler_lock() is True
        await b._release_scheduler_lock()
