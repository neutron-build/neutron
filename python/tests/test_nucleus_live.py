"""Every Nucleus data model, driven through the real client against a real server.

`test_nucleus_models.py` says it in its own first line: "mocked -- no real DB
required". That is why, on 2026-08-11, `Document.get`, `Graph.neighbors`,
`Graph.shortest_path`, `CDC.read`, `TimeSeries.range_count`/`range_avg`,
`Streams.xrange`/`xread`, `Blob.get`/`meta`, `Datalog.query` and the KV range
reads were all found to have NEVER WORKED over pgwire from Python -- while their
mocked tests were green. A mock asserts that the client builds the SQL string
the test expects. It cannot know the server describes that statement with zero
result columns, or types a parameter as TEXT that the client binds as an
integer, and those are the failures that actually happen.

So this suite asserts only what a mock cannot: that the call reaches a live
engine, is accepted over the wire, and comes back with the right value.

    NEUTRON_TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:5432/postgres \
        pytest tests/test_nucleus_live.py

Skips without that variable, matching `test_jobs_durable.py`. **CI must set
it** -- a suite that silently skips is the same as no suite, which is how the
Python live tests came to never run at all.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio

from neutron.nucleus.client import NucleusClient

DATABASE_URL = os.environ.get("NEUTRON_TEST_DATABASE_URL")

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(
        not DATABASE_URL,
        reason="NEUTRON_TEST_DATABASE_URL is not set; these need a live Nucleus",
    ),
]


@pytest_asyncio.fixture
async def client():
    c = await NucleusClient.connect(DATABASE_URL, min_size=1, max_size=4)
    try:
        yield c
    finally:
        # Also exercises the close path: it hung forever until N25 was fixed,
        # because the server never closed the socket after Terminate.
        await c.pool.close()


def _unique(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:10]}"


# ── documents ────────────────────────────────────────────────────────────

async def test_document_round_trip(client):
    coll = _unique("docs")
    doc_id = await client.document.insert(coll, {"title": "hello", "n": 1})

    got = await client.document.get_in(coll, int(doc_id))
    assert got is not None and got["title"] == "hello"

    # update/delete select by FILTER and return a count.
    assert await client.document.update(coll, {"title": "hello"}, {"title": "changed"}) == 1
    assert (await client.document.get_in(coll, int(doc_id)))["title"] == "changed"

    assert await client.document.delete(coll, {"title": "changed"}) == 1
    assert await client.document.get_in(coll, int(doc_id)) is None


async def test_document_path_reads_a_nested_key(client):
    coll = _unique("docs")
    doc_id = await client.document.insert(coll, {"user": {"name": "ada"}})
    got = await client.document.get_path_in(coll, int(doc_id), "user", "name")
    # NOTE: this comes back JSON-encoded ('"ada"', not 'ada') because DOC_PATH
    # returns raw JSON and the Python client does not decode it. Recorded as an
    # SDK inconsistency rather than asserted away -- json.loads here so the test
    # pins the VALUE, and the encoding question stays visible in OPEN_WORK.
    assert json.loads(got) == "ada"


async def test_document_collections_are_isolated(client):
    coll = _unique("coll")
    doc_id = await client.document.insert(coll, {"k": "v"})
    assert (await client.document.get_in(coll, int(doc_id)))["k"] == "v"
    # A document in another collection must read as absent, not leak.
    assert await client.document.get(int(doc_id)) is None


# ── graph ────────────────────────────────────────────────────────────────

async def test_graph_neighbors_and_shortest_path(client):
    a = await client.graph.add_node(["person"], {"name": "a"})
    b = await client.graph.add_node(["person"], {"name": "b"})
    await client.graph.add_edge("knows", a, b)

    neighbors = await client.graph.neighbors(a, direction="out")
    assert neighbors, "GRAPH_NEIGHBORS returned nothing for a node with an out edge"

    path = await client.graph.shortest_path(a, b)
    assert path, "GRAPH_SHORTEST_PATH found no path between two connected nodes"


# ── key/value ────────────────────────────────────────────────────────────

async def test_kv_scalar_and_ttl(client):
    key = _unique("k")
    await client.kv.set(key, "v")
    assert await client.kv.get(key) == "v"
    assert await client.kv.expire(key, 60) is True
    assert await client.kv.delete(key) is True
    assert await client.kv.get(key) is None


async def test_kv_list_range_reads(client):
    key = _unique("list")
    await client.kv.rpush(key, "a")
    await client.kv.rpush(key, "b")
    assert await client.kv.lrange(key, 0, 10) == ["a", "b"]
    assert await client.kv.lindex(key, 0) == "a"


async def test_kv_sorted_set_range_reads(client):
    key = _unique("zset")
    await client.kv.zadd(key, 1.0, "one")
    await client.kv.zadd(key, 2.0, "two")
    # Explicit upper bound, not Redis's -1: the engine reads -1 literally and
    # returns nothing. Recorded in OPEN_WORK; asserted here as it behaves.
    assert await client.kv.zrange(key, 0, 10) == ["one:1.0", "two:2.0"]


# ── time series ──────────────────────────────────────────────────────────

def _points():
    from neutron.nucleus.timeseries import TimeSeriesPoint

    base = datetime(2026, 8, 11, 12, 0, 0, tzinfo=timezone.utc)
    return base, [
        TimeSeriesPoint(timestamp=base, value=10.0),
        TimeSeriesPoint(timestamp=base + timedelta(seconds=1), value=20.0),
    ]


async def test_timeseries_write_and_count(client):
    measurement = _unique("m")
    _, points = _points()
    await client.timeseries.write(measurement, points)
    assert await client.timeseries.count(measurement) == 2
    assert await client.timeseries.last(measurement) is not None


@pytest.mark.xfail(
    reason="TimeSeries.aggregate is broken in the SDK, not the engine: it calls "
    "TIME_BUCKET($1, $2) with an interval NAME ('hour') while the engine's "
    "TIME_BUCKET takes bucket_millis. It has never worked. Left xfail so the "
    "suite stays green and the breakage stays visible; see OPEN_WORK.",
    strict=True,
)
async def test_timeseries_aggregate_over_a_range(client):
    measurement = _unique("m")
    base, points = _points()
    await client.timeseries.write(measurement, points)
    # TS_RANGE_COUNT / TS_RANGE_AVG: both described zero result columns and
    # took their bounds as TEXT before 2026-08-11.
    out = await client.timeseries.aggregate(
        measurement, base - timedelta(minutes=1), base + timedelta(minutes=1),
        timedelta(minutes=5),
    )
    assert out is not None


# ── streams ──────────────────────────────────────────────────────────────

async def test_streams_append_and_range(client):
    stream = _unique("s")
    await client.streams.xadd(stream, {"v": "1"})
    await client.streams.xadd(stream, {"v": "2"})
    assert await client.streams.xlen(stream) == 2
    entries = await client.streams.xrange(stream, 0, 9_999_999_999_999, 10)
    assert len(entries) == 2


# ── blobs ────────────────────────────────────────────────────────────────

async def test_blob_round_trip(client):
    bucket, key = _unique("b"), _unique("blob")
    await client.blob.put(bucket, key, b"payload")
    assert await client.blob.get(bucket, key) == b"payload"
    assert await client.blob.get_meta(bucket, key) is not None
    assert await client.blob.delete(bucket, key) is True
    assert await client.blob.get(bucket, key) is None


# ── change data capture ──────────────────────────────────────────────────

async def test_cdc_read_returns_a_list(client):
    # The value matters less than the call surviving the wire: this described
    # zero result columns, so asyncpg refused every response.
    events = await client.cdc.read(0, 10)
    assert isinstance(events, list)


# ── full-text search ─────────────────────────────────────────────────────

async def test_fts_index_and_search(client):
    index = _unique("idx")
    await client.fts.index_doc(index, "1", {"body": "the quick brown fox"})
    hits = await client.fts.search(index, "quick", limit=10)
    assert hits, "FTS_SEARCH found nothing for a term that was just indexed"


# ── the whole surface, in one place ──────────────────────────────────────

async def test_feature_detection_reports_nucleus(client):
    # If this says PostgreSQL, everything above tested the wrong server and the
    # specialty assertions would be silently meaningless.
    assert client.features.is_nucleus is True
