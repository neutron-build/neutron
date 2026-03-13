"""Job queue — enqueue, process, schedule, and retry background tasks."""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from enum import Enum
from typing import Any, Callable, Awaitable

from pydantic import BaseModel


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


class JobQueue:
    """Background job queue with scheduling, retry, and optional persistence.

    Pass a ``NucleusClient`` (or any object with a ``.sql`` attribute) as
    ``db`` to persist jobs across restarts.  Without ``db`` the queue is
    in-memory only.

    Usage::

        queue = JobQueue()

        @queue.handler("send_email")
        async def handle_email(payload: dict) -> None:
            await send_email(**payload)

        job_id = await queue.enqueue("send_email", {"to": "a@b.com"})
        await queue.start_worker(concurrency=4)
    """

    def __init__(self, db: Any = None, retry_delay_base: float = 1.0) -> None:
        self.db = db
        self._retry_delay_base = retry_delay_base
        self._handlers: dict[str, Callable[[dict], Awaitable[Any]]] = {}
        self._jobs: dict[str, Job] = {}
        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._schedules: list[_ScheduledJob] = []
        self._running = False
        self._workers: list[asyncio.Task] = []
        self._db_ready = False

    def handler(self, task: str) -> Callable:
        """Decorator to register a job handler."""

        def decorator(fn: Callable) -> Callable:
            self._handlers[task] = fn
            return fn

        return decorator

    async def _ensure_db(self) -> None:
        if self._db_ready or self.db is None:
            return
        try:
            await self.db.sql._pool.execute(_CREATE_JOBS_TABLE)
            self._db_ready = True
        except Exception:
            pass  # best-effort; fall back to in-memory

    async def _persist_job(self, job: Job) -> None:
        if self.db is None or not self._db_ready:
            return
        try:
            await self.db.sql._pool.execute(
                """
                INSERT INTO _neutron_jobs
                    (id, task, payload, status, attempts, max_retries,
                     created_at, started_at, completed_at, error, result, scheduled_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
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
            )
        except Exception:
            pass

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
        self._jobs[job_id] = job
        await self._persist_job(job)
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
        self._workers.clear()

    async def _worker_loop(self) -> None:
        """Process jobs from the queue."""
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

            # Honour scheduled_at delay
            if job.scheduled_at and time.time() < job.scheduled_at:
                await self._queue.put(job_id)
                await asyncio.sleep(0.1)
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
        """Check scheduled jobs every minute."""
        while self._running:
            try:
                now = time.localtime()
                for sched in self._schedules:
                    if sched.matches(now):
                        await self.enqueue(sched.task, sched.payload)
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break


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
            lo, hi = int(parts[0]), int(parts[1])
        except ValueError:
            return False
        return lo <= value <= hi

    # Exact value
    try:
        return int(field) == value
    except ValueError:
        return False
