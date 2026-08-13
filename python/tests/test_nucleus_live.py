"""Every Nucleus data model, driven through the real client against a real server.

The cases are NOT written here. They live in `conformance/live/spec.json`, and
this file is the pytest face of the shared executor in
`conformance/live/executors/python/`. Seven SDKs run the same spec; a case
written in one language's test file is drift waiting to happen.

`test_nucleus_models.py` says it in its own first line: "mocked -- no real DB
required". That is why, on 2026-08-11, eighteen call shapes were found to have
NEVER WORKED over pgwire from Python while every one of them had a green test. A
mock asserts that the client builds the SQL string the test expects. It cannot
know the server describes that statement with zero result columns, or types a
parameter as TEXT that the client binds as an integer.

    NEUTRON_TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:55432/postgres \
        pytest tests/test_nucleus_live.py

Skips without that variable. **CI must set it** -- a suite that silently skips is
the same as no suite, which is how the Python live tests came to never run at
all. `conformance/live/scripts/start-engine.sh` boots an engine configured for
this, and CI asserts the run did not skip.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
LIVE = REPO / "conformance" / "live"
sys.path.insert(0, str(LIVE / "executors" / "python"))

DATABASE_URL = os.environ.get("NEUTRON_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DATABASE_URL,
    reason="NEUTRON_TEST_DATABASE_URL is not set; these need a live Nucleus",
)

SPEC = json.loads((LIVE / "spec.json").read_text())


@pytest.fixture(scope="module")
def client():
    from neutron.nucleus.client import NucleusClient

    loop = asyncio.new_event_loop()
    c = loop.run_until_complete(
        NucleusClient.connect(DATABASE_URL, min_size=1, max_size=4)
    )
    try:
        yield loop, c
    finally:
        # Also exercises the close path: it hung forever until N25 was fixed,
        # because the server never closed the socket after Terminate.
        loop.run_until_complete(c.pool.close())
        loop.close()


@pytest.mark.parametrize("case", SPEC["cases"], ids=lambda c: c["id"])
def test_case(case, client):
    import run_live

    loop, c = client
    expected_fail = "xfail" in case
    try:
        loop.run_until_complete(run_live.run_case(case, c, DATABASE_URL))
    except Exception as exc:  # noqa: BLE001 — any failure is the result
        if expected_fail:
            pytest.xfail(f"{case['xfail']['reason']} (observed: {exc})")
        raise
    if expected_fail:
        pytest.fail(
            f"{case['id']} is marked xfail but passed. The underlying bug is "
            f"fixed and the note in spec.json is now false: "
            f"{case['xfail']['reason']}"
        )
