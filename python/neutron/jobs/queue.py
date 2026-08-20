"""Job queue — enqueue, process, schedule, and retry background tasks."""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
import uuid
from enum import Enum
from typing import Any, Callable, Awaitable

from pydantic import BaseModel

# Arbitrary but stable: the advisory-lock key the scheduler election uses.
_SCHEDULER_LOCK_KEY = 0x6E65_7574


class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"


class Job(BaseModel):
    id: str
    task: str
    payload: dict[str, Any] = {}
    status: JobStatus = JobStatus.PENDING
    attempts: int = 0
    max_retries: int = 3
    created_at: float = 0.0
    started_at: float | None = None
    completed_at: float | None = None
    error: str | None = None
    result: Any = None
    scheduled_at: float | None = None


# SQL schema for persistent job storage (created on first use)
_CREATE_JOBS_TABLE = """
CREATE TABLE IF NOT EXISTS _neutron_jobs (
    id TEXT PRIMARY KEY,
    task TEXT NOT NULL,
    payload JSONB DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    created_at DOUBLE PRECISION NOT NULL,
    started_at DOUBLE PRECISION,
    completed_at DOUBLE PRECISION,
    error TEXT,
    result JSONB,
    scheduled_at DOUBLE PRECISION
)
"""

# Lease columns, added separately so an existing deployment upgrades in place.
_ADD_LEASE_COLUMNS = """
ALTER TABLE _neutron_jobs
    ADD COLUMN IF NOT EXISTS available_at DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS lease_owner TEXT,
    ADD COLUMN IF NOT EXISTS lease_expires_at DOUBLE PRECISION
"""

_CREATE_CLAIM_INDEX = """
CREATE INDEX IF NOT EXISTS _neutron_jobs_claimable
    ON _neutron_jobs (available_at)
    WHERE status IN ('pending', 'retrying', 'running')
"""

# Claim exactly one job. SKIP LOCKED lets N workers claim concurrently without
# blocking each other; the subquery is what makes the claim atomic.
#
# Two things are claimable: a job that is due, and a job whose worker died --
# detected by an expired lease rather than by a heartbeat table, so a hard kill
# recovers without cooperation from the dead process.
_CLAIM_JOB = """
UPDATE _neutron_jobs SET
    status = 'running',
    lease_owner = $1,
    lease_expires_at = $2 + $3,
    started_at = $2,
    attempts = attempts + 1
WHERE id = (
    SELECT id FROM _neutron_jobs
    WHERE (
            status IN ('pending', 'retrying')
            AND coalesce(available_at, created_at) <= $2
          )
       OR (status = 'running' AND lease_expires_at < $2)
    ORDER BY coalesce(available_at, created_at)
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING id, task, payload, status, attempts, max_retries, created_at,
          started_at, completed_at, error, result, scheduled_at
"""

_HEARTBEAT = """
UPDATE _neutron_jobs SET lease_expires_at = $1
WHERE id = $2 AND lease_owner = $3
"""

# Every terminal write is guarded by lease_owner. A worker whose lease expired
# and was stolen must not overwrite the new owner's result.
_FINISH_JOB = """
UPDATE _neutron_jobs SET
    status = $1, completed_at = $2, error = $3, result = $4::jsonb,
    lease_owner = NULL, lease_expires_at = NULL
WHERE id = $5 AND lease_owner = $6
"""

_RETRY_JOB = """
UPDATE _neutron_jobs SET
    status = 'retrying', error = $1, available_at = $2,
    lease_owner = NULL, lease_expires_at = NULL
WHERE id = $3 AND lease_owner = $4
"""


class JobQueue:
    """Background job queue with scheduling, retry, and optional durability.

    Two modes, and the difference matters:

    **In-memory** (``db=None``, the default). The queue lives in this process.
    Jobs are lost on restart and are not visible to other processes. Fine for
    a single-process app; not a work queue.

    **Durable** (``db`` set to a ``NucleusClient``, or anything exposing
    ``.pool`` or ``.sql._pool``). The ``_neutron_jobs`` table is the source of
    truth. Workers in any number of processes claim jobs with ``FOR UPDATE
    SKIP LOCKED``, so a job is delivered to exactly one worker. Each claim
    takes a lease which the worker heartbeats; if the worker dies the lease
    expires and another worker picks the job up. Pending work survives a
    restart because the queue is read from the table, not from memory. Exactly
    one instance runs the cron scheduler, elected by a Postgres advisory lock.

    Setup errors are raised rather than swallowed — a queue asked for
    durability will not silently degrade to in-memory.

    Usage::

        queue = JobQueue()

        @queue.handler("send_email")
        async def handle_email(payload: dict) -> None:
            await send_email(**payload)

        job_id = await queue.enqueue("send_email", {"to": "a@b.com"})
        await queue.start_worker(concurrency=4)
    """

    def __init__(
        self,
        db: Any = None,
        retry_delay_base: float = 1.0,
        *,
        lease_ttl: float = 60.0,
        poll_interval: float = 0.5,
        worker_id: str | None = None,
    ) -> None:
        self.db = db
        self._retry_delay_base = retry_delay_base
        self._lease_ttl = lease_ttl
        self._poll_interval = poll_interval
        self._worker_id = worker_id or f"{uuid.uuid4()}"
        self._handlers: dict[str, Callable[[dict], Awaitable[Any]]] = {}
        self._jobs: dict[str, Job] = {}
        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._schedules: list[_ScheduledJob] = []
        self._running = False
        self._workers: list[asyncio.Task] = []
        self._db_ready = False
        self._scheduler_conn: Any = None

    @property
    def durable(self) -> bool:
        """True when Postgres is the source of truth rather than this process."""
        return self.db is not None

    def _pool(self) -> Any:
        pool = getattr(self.db, "pool", None)
        if pool is None:
            pool = self.db.sql._pool
        return pool

    def handler(self, task: str) -> Callable:
        """Decorator to register a job handler."""

        def decorator(fn: Callable) -> Callable:
            self._handlers[task] = fn
            return fn

        return decorator

    async def _ensure_db(self) -> None:
        """Create the jobs table and lease columns.

        Errors are raised, not swallowed. A queue configured for durability
        that silently degrades to in-memory is worse than one that refuses to
        start, because the data loss is only discovered after a restart.
        """
        if self._db_ready or self.db is None:
            return
        pool = self._pool()
        await pool.execute(_CREATE_JOBS_TABLE)
        await pool.execute(_ADD_LEASE_COLUMNS)
        await pool.execute(_CREATE_CLAIM_INDEX)
        self._db_ready = True

    async def _persist_job(self, job: Job) -> None:
        if self.db is None:
            return
        await self._pool().execute(
            """
            INSERT INTO _neutron_jobs
                (id, task, payload, status, attempts, max_retries,
                 created_at, started_at, completed_at, error, result,
                 scheduled_at, available_at)
            VALUES ($1,$2,$3::jsonb,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12,$13)
            ON CONFLICT (id) DO UPDATE SET
                status=$4, attempts=$5, started_at=$8,
                completed_at=$9, error=$10, result=$11
            """,
            job.id,
            job.task,
            json.dumps(job.payload),
            job.status,
            job.attempts,
            job.max_retries,
            job.created_at,
            job.started_at,
            job.completed_at,
            job.error,
            json.dumps(job.result) if job.result is not None else None,
            job.scheduled_at,
            job.scheduled_at if job.scheduled_at else job.created_at,
        )

    async def enqueue(
        self,
        task: str,
        payload: dict[str, Any] | None = None,
        *,
        max_retries: int = 3,
        delay: float = 0,
    ) -> str:
        """Add a job to the queue. Returns the job ID."""
        await self._ensure_db()
        job_id = str(uuid.uuid4())
        now = time.time()
        job = Job(
            id=job_id,
            task=task,
            payload=payload or {},
            status=JobStatus.PENDING,
            max_retries=max_retries,
            created_at=now,
            scheduled_at=now + delay if delay > 0 else None,
        )
        await self._persist_job(job)
        if not self.durable:
            # In-memory mode only: the asyncio queue IS the queue. In durable
            # mode the row is the queue, so workers in any process can claim it.
            self._jobs[job_id] = job
            await self._queue.put(job_id)
        return job_id

    async def schedule(
        self,
        cron_expr: str,
        task: str,
        payload: dict[str, Any] | None = None,
    ) -> None:
        """Schedule a recurring job using a cron expression.

        Supported format: ``minute hour day_of_month month day_of_week``
        Supports ``*``, exact values, comma lists ``1,3,5``, ranges ``1-5``,
        and step values ``*/15``.
        """
        self._schedules.append(
            _ScheduledJob(cron=cron_expr, task=task, payload=payload or {})
        )

    def get_job(self, job_id: str) -> Job | None:
        """Get a job by ID."""
        return self._jobs.get(job_id)

    async def start_worker(self, *, concurrency: int = 1) -> None:
        """Start background worker(s). Runs until ``stop_worker()`` is called."""
        await self._ensure_db()
        self._running = True
        for _ in range(concurrency):
            task = asyncio.create_task(self._worker_loop())
            self._workers.append(task)

        if self._schedules:
            self._workers.append(asyncio.create_task(self._scheduler_loop()))

    async def stop_worker(self) -> None:
        """Stop all workers gracefully."""
        self._running = False
        for task in self._workers:
            task.cancel()
        for task in self._workers:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        self._workers.clear()
        await self._release_scheduler_lock()

    async def _acquire_scheduler_lock(self) -> bool:
        """Elect a single scheduler across processes.

        Without this, every instance fires every cron entry, so a two-replica
        deployment silently doubles all scheduled work.
        """
        if not self.durable:
            return True
        self._scheduler_conn = await self._pool().acquire()
        got = await self._scheduler_conn.fetchval(
            "SELECT pg_try_advisory_lock($1)", _SCHEDULER_LOCK_KEY
        )
        if not got:
            await self._pool().release(self._scheduler_conn)
            self._scheduler_conn = None
        return bool(got)

    async def _release_scheduler_lock(self) -> None:
        if self._scheduler_conn is None:
            return
        conn, self._scheduler_conn = self._scheduler_conn, None
        with contextlib.suppress(Exception):
            await conn.execute("SELECT pg_advisory_unlock($1)", _SCHEDULER_LOCK_KEY)
        with contextlib.suppress(Exception):
            await self._pool().release(conn)

    async def fetch_job(self, job_id: str) -> Job | None:
        """Read a job from the durable store.

        ``get_job`` only sees this process's memory; in durable mode a job
        enqueued elsewhere is not there.
        """
        if not self.durable:
            return self._jobs.get(job_id)
        await self._ensure_db()
        row = await self._pool().fetchrow(
            "SELECT id, task, payload, status, attempts, max_retries, created_at,"
            " started_at, completed_at, error, result, scheduled_at"
            " FROM _neutron_jobs WHERE id = $1",
            job_id,
        )
        return _row_to_job(row) if row else None

    async def _claim(self) -> Job | None:
        now = time.time()
        row = await self._pool().fetchrow(
            _CLAIM_JOB, self._worker_id, now, self._lease_ttl
        )
        return _row_to_job(row) if row else None

    async def _heartbeat(self, job_id: str) -> None:
        """Extend the lease while the handler runs, so a slow job is not stolen."""
        interval = max(self._lease_ttl / 3.0, 0.1)
        while True:
            await asyncio.sleep(interval)
            await self._pool().execute(
                _HEARTBEAT, time.time() + self._lease_ttl, job_id, self._worker_id
            )

    async def _durable_worker_loop(self) -> None:
        while self._running:
            try:
                job = await self._claim()
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(self._poll_interval)
                continue

            if job is None:
                try:
                    await asyncio.sleep(self._poll_interval)
                except asyncio.CancelledError:
                    break
                continue

            await self._run_claimed(job)

    async def _run_claimed(self, job: Job) -> None:
        pool = self._pool()
        handler = self._handlers.get(job.task)
        if handler is None:
            await pool.execute(
                _FINISH_JOB,
                JobStatus.FAILED.value,
                time.time(),
                f"No handler registered for task: {job.task}",
                None,
                job.id,
                self._worker_id,
            )
            return

        beat = asyncio.create_task(self._heartbeat(job.id))
        try:
            result = await handler(job.payload)
        except asyncio.CancelledError:
            beat.cancel()
            raise
        except Exception as exc:
            beat.cancel()
            if job.attempts < job.max_retries:
                backoff = self._retry_delay_base * (2 ** (job.attempts - 1))
                await pool.execute(
                    _RETRY_JOB, str(exc), time.time() + backoff, job.id,
                    self._worker_id,
                )
            else:
                await pool.execute(
                    _FINISH_JOB, JobStatus.FAILED.value, time.time(), str(exc),
                    None, job.id, self._worker_id,
                )
            return
        else:
            beat.cancel()
            await pool.execute(
                _FINISH_JOB,
                JobStatus.COMPLETED.value,
                time.time(),
                None,
                json.dumps(result) if result is not None else None,
                job.id,
                self._worker_id,
            )
        finally:
            with contextlib.suppress(asyncio.CancelledError):
                await beat

    async def _worker_loop(self) -> None:
        """Process jobs from the queue."""
        if self.durable:
            await self._durable_worker_loop()
            return
        while self._running:
            try:
                job_id = await asyncio.wait_for(self._queue.get(), timeout=1.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                if not self._running:
                    break
                continue

            job = self._jobs.get(job_id)
            if job is None:
                continue

            # Honour scheduled_at delay. Sleeping until the job is actually due
            # rather than re-queueing every 100ms, so a backlog of delayed jobs
            # does not spin every worker.
            if job.scheduled_at and time.time() < job.scheduled_at:
                await asyncio.sleep(
                    min(job.scheduled_at - time.time(), self._poll_interval)
                )
                await self._queue.put(job_id)
                continue

            handler = self._handlers.get(job.task)
            if handler is None:
                job.status = JobStatus.FAILED
                job.error = f"No handler registered for task: {job.task}"
                await self._persist_job(job)
                continue

            job.status = JobStatus.RUNNING
            job.started_at = time.time()
            job.attempts += 1
            await self._persist_job(job)

            try:
                result = await handler(job.payload)
                job.status = JobStatus.COMPLETED
                job.completed_at = time.time()
                job.result = result
                await self._persist_job(job)
            except Exception as e:
                job.error = str(e)
                if job.attempts < job.max_retries:
                    job.status = JobStatus.RETRYING
                    # Exponential backoff: base * 2^(attempts-1)
                    backoff = self._retry_delay_base * (2 ** (job.attempts - 1))
                    job.scheduled_at = time.time() + backoff
                    await self._persist_job(job)
                    await self._queue.put(job_id)
                else:
                    job.status = JobStatus.FAILED
                    job.completed_at = time.time()
                    await self._persist_job(job)

    async def _scheduler_loop(self) -> None:
        """Check scheduled jobs every minute, on exactly one instance.

        Fires at most once per wall-clock minute per schedule: matching on
        ``time.localtime()`` alone would double-fire when an iteration takes
        under a second, and skip a minute when one takes over sixty.
        """
        if not await self._acquire_scheduler_lock():
            return
        last_fired: dict[int, tuple] = {}
        try:
            while self._running:
                try:
                    now = time.localtime()
                    minute = now[:5]
                    for index, sched in enumerate(self._schedules):
                        if sched.matches(now) and last_fired.get(index) != minute:
                            last_fired[index] = minute
                            await self.enqueue(sched.task, sched.payload)
                    await asyncio.sleep(1.0 if self._running else 0)
                except asyncio.CancelledError:
                    break
        finally:
            await self._release_scheduler_lock()


def _row_to_job(row: Any) -> Job:
    payload = row["payload"]
    result = row["result"]
    return Job(
        id=row["id"],
        task=row["task"],
        payload=json.loads(payload) if isinstance(payload, str) else (payload or {}),
        status=JobStatus(row["status"]),
        attempts=row["attempts"],
        max_retries=row["max_retries"],
        created_at=row["created_at"],
        started_at=row["started_at"],
        completed_at=row["completed_at"],
        error=row["error"],
        result=json.loads(result) if isinstance(result, str) else result,
        scheduled_at=row["scheduled_at"],
    )


class _ScheduledJob:
    """Represents a cron-scheduled recurring job."""

    def __init__(self, cron: str, task: str, payload: dict[str, Any]) -> None:
        self.cron = cron
        self.task = task
        self.payload = payload
        self._parse_cron(cron)

    def _parse_cron(self, expr: str) -> None:
        parts = expr.split()
        if len(parts) != 5:
            raise ValueError(f"Invalid cron expression: {expr!r} (expected 5 fields)")
        self.minute = parts[0]
        self.hour = parts[1]
        self.day = parts[2]
        self.month = parts[3]
        self.weekday = parts[4]

    def matches(self, t: time.struct_time) -> bool:
        """Check if the current time matches this cron schedule."""
        return (
            _field_matches(self.minute, t.tm_min)
            and _field_matches(self.hour, t.tm_hour)
            and _field_matches(self.day, t.tm_mday)
            and _field_matches(self.month, t.tm_mon)
            and _field_matches(self.weekday, (t.tm_wday + 1) % 7)  # cron: 0=Sun
        )


def _field_matches(field: str, value: int) -> bool:
    """Evaluate a single cron field against an integer value.

    Supports: ``*``, exact values, ``*/N`` step, ``a-b`` range,
    and ``a,b,c`` comma lists (including ranges within lists).
    """
    if field == "*":
        return True

    # Comma list: any element must match
    if "," in field:
        return any(_field_matches(part.strip(), value) for part in field.split(","))

    # Step: */N or start-end/N
    if "/" in field:
        base, step_str = field.rsplit("/", 1)
        try:
            step = int(step_str)
        except ValueError:
            return False
        if base == "*":
            return value % step == 0
        # range/step
        if "-" in base:
            lo, hi = base.split("-", 1)
            try:
                lo_i, hi_i = int(lo), int(hi)
            except ValueError:
                return False
            return lo_i <= value <= hi_i and (value - lo_i) % step == 0
        try:
            start = int(base)
        except ValueError:
            return False
        return value >= start and (value - start) % step == 0

    # Range: a-b
    if "-" in field:
        parts = field.split("-", 1)
        try:
            lo_n, hi_n = int(parts[0]), int(parts[1])
        except ValueError:
            return False
        return lo_n <= value <= hi_n

    # Exact value
    try:
        return int(field) == value
    except ValueError:
        return False
