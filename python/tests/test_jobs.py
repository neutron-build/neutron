"""Tests for neutron/jobs — job queue, scheduling, retry."""

from __future__ import annotations

import asyncio
import time

import pytest

from neutron.jobs.queue import JobQueue, Job, JobStatus, _ScheduledJob


class TestJobQueue:
    async def test_enqueue_and_process(self):
        queue = JobQueue()
        results = []

        @queue.handler("greet")
        async def handle_greet(payload: dict) -> str:
            results.append(f"Hello {payload['name']}")
            return f"Hello {payload['name']}"

        job_id = await queue.enqueue("greet", {"name": "Alice"})
        assert job_id

        job = queue.get_job(job_id)
        assert job is not None
        assert job.status == JobStatus.PENDING

        # Process manually by running worker briefly
        await queue.start_worker()
        await asyncio.sleep(0.2)
        await queue.stop_worker()

        job = queue.get_job(job_id)
        assert job.status == JobStatus.COMPLETED
        assert len(results) == 1
        assert results[0] == "Hello Alice"

    async def test_job_retry_on_failure(self):
        # Use tiny retry_delay_base so backoffs (0.01s, 0.02s) complete well
        # within the 0.5s sleep window.
        queue = JobQueue(retry_delay_base=0.01)
        attempt_count = 0

        @queue.handler("flaky")
        async def handle_flaky(payload: dict) -> str:
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < 3:
                raise ValueError("Not ready yet")
            return "done"

        job_id = await queue.enqueue("flaky", {}, max_retries=3)

        await queue.start_worker()
        await asyncio.sleep(0.5)
        await queue.stop_worker()

        job = queue.get_job(job_id)
        assert job.status == JobStatus.COMPLETED
        assert attempt_count == 3

    async def test_job_fails_after_max_retries(self):
        queue = JobQueue(retry_delay_base=0.01)

        @queue.handler("always_fail")
        async def handle_fail(payload: dict) -> None:
            raise ValueError("Permanent failure")

        job_id = await queue.enqueue("always_fail", {}, max_retries=2)

        await queue.start_worker()
        await asyncio.sleep(0.5)
        await queue.stop_worker()

        job = queue.get_job(job_id)
        assert job.status == JobStatus.FAILED
        assert "Permanent failure" in job.error
        assert job.attempts == 2

    async def test_unknown_handler(self):
        queue = JobQueue()
        job_id = await queue.enqueue("nonexistent", {})

        await queue.start_worker()
        await asyncio.sleep(0.2)
        await queue.stop_worker()

        job = queue.get_job(job_id)
        assert job.status == JobStatus.FAILED
        assert "No handler" in job.error

    async def test_multiple_jobs(self):
        queue = JobQueue()
        results = []

        @queue.handler("add")
        async def handle_add(payload: dict) -> int:
            result = payload["a"] + payload["b"]
            results.append(result)
            return result

        await queue.enqueue("add", {"a": 1, "b": 2})
        await queue.enqueue("add", {"a": 3, "b": 4})
        await queue.enqueue("add", {"a": 5, "b": 6})

        await queue.start_worker()
        await asyncio.sleep(0.3)
        await queue.stop_worker()

        assert sorted(results) == [3, 7, 11]

    async def test_concurrent_workers(self):
        queue = JobQueue()
        results = []

        @queue.handler("slow")
        async def handle_slow(payload: dict) -> None:
            await asyncio.sleep(0.1)
            results.append(payload["id"])

        for i in range(4):
            await queue.enqueue("slow", {"id": i})

        await queue.start_worker(concurrency=4)
        await asyncio.sleep(0.5)
        await queue.stop_worker()

        assert len(results) == 4

    def test_get_job_missing(self):
        queue = JobQueue()
        assert queue.get_job("nonexistent") is None

    async def test_job_result_stored(self):
        queue = JobQueue()

        @queue.handler("compute")
        async def handle(payload: dict) -> dict:
            return {"answer": 42}

        job_id = await queue.enqueue("compute", {})

        await queue.start_worker()
        await asyncio.sleep(0.2)
        await queue.stop_worker()

        job = queue.get_job(job_id)
        assert job.result == {"answer": 42}


class TestScheduledJob:
    def test_cron_parse(self):
        sched = _ScheduledJob("0 9 * * *", "daily", {})
        assert sched.minute == "0"
        assert sched.hour == "9"

    def test_cron_invalid(self):
        with pytest.raises(ValueError, match="Invalid cron"):
            _ScheduledJob("bad", "task", {})

    def test_cron_matches(self):
        sched = _ScheduledJob("* * * * *", "always", {})
        assert sched.matches(time.localtime()) is True

    def test_cron_step(self):
        sched = _ScheduledJob("*/5 * * * *", "every5", {})
        t = time.struct_time((2026, 1, 1, 0, 0, 0, 0, 0, 0))
        assert sched.matches(t) is True
        t = time.struct_time((2026, 1, 1, 0, 3, 0, 0, 0, 0))
        assert sched.matches(t) is False


class TestJobModel:
    def test_job_defaults(self):
        job = Job(id="j1", task="test")
        assert job.status == JobStatus.PENDING
        assert job.attempts == 0
        assert job.max_retries == 3


class TestJobsExports:
    def test_all_exports(self):
        from neutron.jobs import JobQueue, Job, JobStatus
        assert JobQueue is not None
        assert Job is not None
        assert JobStatus is not None
