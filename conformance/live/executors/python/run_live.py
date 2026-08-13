#!/usr/bin/env python3
"""Python executor for the Nucleus live data-model conformance spec.

Reads ../../spec.json, runs every case against a live engine through the real
`neutron` client, and prints one JSON result document to stdout. It asserts
nothing a mock could assert: only that a call reaches the engine, is accepted
over the wire, and comes back with the right value.

    NEUTRON_TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:55432/postgres \
        python3 run_live.py

Exit codes: 0 all cases behaved as the spec says, 1 otherwise. An `xfail` case
that PASSES is a failure — otherwise a fix lands and the note explaining why the
case is expected to fail quietly becomes a lie.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import re
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

SPEC = Path(__file__).resolve().parents[2] / "spec.json"
UNSUPPORTED = Path(__file__).resolve().parent / "unsupported.json"
DATABASE_URL = os.environ.get("NEUTRON_TEST_DATABASE_URL")

# Time-series timestamps in the spec are millisecond offsets from this instant.
# A fixed base keeps the cases deterministic and comparable across SDKs.
TS_BASE = datetime(2026, 8, 11, 12, 0, 0, tzinfo=timezone.utc)

FIXTURE_RE = re.compile(r"@([A-Za-z_][A-Za-z0-9_]*)")


class Unsupported(Exception):
    """The SDK has no surface for this op."""


def resolve(value: Any, fixtures: dict[str, str], bound: dict[str, Any]) -> Any:
    if isinstance(value, str):
        if value.startswith("$"):
            name = value[1:]
            if name not in bound:
                raise KeyError(f"step references ${name} before it was bound")
            return bound[name]
        return FIXTURE_RE.sub(lambda m: fixtures.setdefault(
            m.group(1), f"{m.group(1)}_{uuid.uuid4().hex[:10]}"
        ), value)
    if isinstance(value, list):
        return [resolve(v, fixtures, bound) for v in value]
    if isinstance(value, dict):
        return {k: resolve(v, fixtures, bound) for k, v in value.items()}
    return value


def check(result: Any, expect: dict[str, Any]) -> None:
    actual = result
    if "key" in expect:
        if actual is None:
            raise AssertionError(f"expected a map with key {expect['key']!r}, got None")
        actual = actual[expect["key"]] if isinstance(actual, dict) else getattr(actual, expect["key"])
    if "index" in expect:
        actual = actual[expect["index"]]
    if expect.get("jsonDecode"):
        actual = json.loads(actual)

    if expect.get("notNull"):
        assert actual is not None, "expected a value, got None"
    if expect.get("isNull"):
        assert actual is None, f"expected None, got {actual!r}"
    if expect.get("nonEmpty"):
        assert actual, f"expected a non-empty collection, got {actual!r}"
    if "length" in expect:
        assert len(actual) == expect["length"], (
            f"expected {expect['length']} elements, got {len(actual)}: {actual!r}"
        )
    if "type" in expect:
        kinds = {
            "list": list, "map": dict, "string": str,
            "int": int, "float": (int, float), "bool": bool, "bytes": bytes,
        }
        assert isinstance(actual, kinds[expect["type"]]), (
            f"expected {expect['type']}, got {type(actual).__name__}: {actual!r}"
        )
    if "equals" in expect:
        want = expect["equals"]
        # Floats compare loosely; everything else exactly.
        if isinstance(want, float) or isinstance(actual, float):
            assert abs(float(actual) - float(want)) < 1e-9, f"expected {want!r}, got {actual!r}"
        else:
            assert actual == want, f"expected {want!r}, got {actual!r}"


class Ops:
    """Maps spec op names onto the Python SDK. One method per op, no cleverness."""

    def __init__(self, client: Any, url: str) -> None:
        self.c = client
        self.url = url

    async def call(self, op: str, args: list[Any]) -> Any:
        fn = getattr(self, op.replace(".", "_"), None)
        if fn is None:
            raise Unsupported(op)
        return await fn(*args)

    # ── core ─────────────────────────────────────────────────────────────
    async def features_isNucleus(self) -> bool:
        return self.c.features.is_nucleus

    async def connection_closeAndReconnect(self) -> bool:
        from neutron.nucleus.client import NucleusClient
        probe = await NucleusClient.connect(self.url, min_size=1, max_size=1)
        # Hung forever before N25: the server ignored Terminate and never closed
        # the socket. asyncio.wait_for turns that hang into a failure.
        await asyncio.wait_for(probe.pool.close(), timeout=15)
        return True

    # ── document ─────────────────────────────────────────────────────────
    async def document_insert(self, coll: str, doc: dict) -> str:
        return await self.c.document.insert(coll, doc)

    async def document_get(self, doc_id: str) -> Any:
        return await self.c.document.get(int(doc_id))

    async def document_getIn(self, coll: str, doc_id: str) -> Any:
        return await self.c.document.get_in(coll, int(doc_id))

    async def document_getPathIn(self, coll: str, doc_id: str, *keys: str) -> Any:
        return await self.c.document.get_path_in(coll, int(doc_id), *keys)

    async def document_update(self, coll: str, flt: dict, patch: dict) -> int:
        return await self.c.document.update(coll, flt, patch)

    async def document_delete(self, coll: str, flt: dict) -> int:
        return await self.c.document.delete(coll, flt)

    async def document_countIn(self, coll: str) -> int:
        return await self.c.document.count_in(coll)

    async def document_find(self, coll: str, flt: dict) -> list:
        return await self.c.document.find(coll, flt)

    async def document_findOne(self, coll: str, flt: dict) -> Any:
        return await self.c.document.find_one(coll, flt)

    # ── graph ────────────────────────────────────────────────────────────
    async def graph_addNode(self, labels: list, props: dict) -> str:
        return await self.c.graph.add_node(labels, props)

    async def graph_addEdge(self, edge_type: str, a: str, b: str) -> str:
        return await self.c.graph.add_edge(edge_type, a, b)

    async def graph_neighbors(self, node_id: str, direction: str) -> list:
        return await self.c.graph.neighbors(node_id, direction=direction)

    async def graph_shortestPath(self, a: str, b: str) -> list:
        return await self.c.graph.shortest_path(a, b)

    async def graph_nodeCount(self) -> int:
        return await self.c.graph.node_count()

    async def graph_edgeCount(self) -> int:
        return await self.c.graph.edge_count()

    async def graph_deleteNode(self, node_id: str) -> bool:
        return await self.c.graph.delete_node(node_id)

    # ── key/value ────────────────────────────────────────────────────────
    async def kv_set(self, key: str, value: str) -> Any:
        return await self.c.kv.set(key, value)

    async def kv_get(self, key: str) -> Any:
        return await self.c.kv.get(key)

    async def kv_exists(self, key: str) -> bool:
        return await self.c.kv.exists(key)

    async def kv_delete(self, key: str) -> bool:
        return await self.c.kv.delete(key)

    async def kv_expire(self, key: str, ttl: int) -> bool:
        return await self.c.kv.expire(key, ttl)

    async def kv_ttl(self, key: str) -> int:
        return await self.c.kv.ttl(key)

    async def kv_incr(self, key: str, delta: int) -> int:
        return await self.c.kv.incr(key, delta)

    async def kv_rpush(self, key: str, value: str) -> int:
        return await self.c.kv.rpush(key, value)

    async def kv_llen(self, key: str) -> int:
        return await self.c.kv.llen(key)

    async def kv_lrange(self, key: str, start: int, stop: int) -> list:
        return await self.c.kv.lrange(key, start, stop)

    async def kv_lindex(self, key: str, index: int) -> Any:
        return await self.c.kv.lindex(key, index)

    async def kv_zadd(self, key: str, score: float, member: str) -> Any:
        return await self.c.kv.zadd(key, score, member)

    async def kv_zrange(self, key: str, start: int, stop: int) -> list:
        return await self.c.kv.zrange(key, start, stop)

    async def kv_hset(self, key: str, field: str, value: str) -> bool:
        return await self.c.kv.hset(key, field, value)

    async def kv_hget(self, key: str, field: str) -> Any:
        return await self.c.kv.hget(key, field)

    async def kv_hexists(self, key: str, field: str) -> bool:
        return await self.c.kv.hexists(key, field)

    async def kv_hgetall(self, key: str) -> dict:
        return await self.c.kv.hgetall(key)

    async def kv_hlen(self, key: str) -> int:
        return await self.c.kv.hlen(key)

    async def kv_hdel(self, key: str, field: str) -> bool:
        return await self.c.kv.hdel(key, field)

    async def kv_sadd(self, key: str, member: str) -> bool:
        return await self.c.kv.sadd(key, member)

    async def kv_srem(self, key: str, member: str) -> bool:
        return await self.c.kv.srem(key, member)

    async def kv_smembers(self, key: str) -> list:
        return await self.c.kv.smembers(key)

    # ── time series ──────────────────────────────────────────────────────
    async def timeseries_write(self, measurement: str, points: list) -> Any:
        from neutron.nucleus.timeseries import TimeSeriesPoint
        return await self.c.timeseries.write(measurement, [
            TimeSeriesPoint(
                timestamp=TS_BASE + timedelta(milliseconds=p["t"]),
                value=float(p["v"]),
            )
            for p in points
        ])

    async def timeseries_count(self, measurement: str) -> int:
        return await self.c.timeseries.count(measurement)

    async def timeseries_last(self, measurement: str) -> Any:
        return await self.c.timeseries.last(measurement)

    async def timeseries_query(self, measurement: str, start_ms: int, end_ms: int) -> list:
        return await self.c.timeseries.query(
            measurement,
            TS_BASE + timedelta(milliseconds=start_ms),
            TS_BASE + timedelta(milliseconds=end_ms),
        )

    async def timeseries_aggregate(
        self, measurement: str, start_ms: int, end_ms: int, window_ms: int
    ) -> list:
        return await self.c.timeseries.aggregate(
            measurement,
            TS_BASE + timedelta(milliseconds=start_ms),
            TS_BASE + timedelta(milliseconds=end_ms),
            timedelta(milliseconds=window_ms),
        )

    # ── streams ──────────────────────────────────────────────────────────
    async def streams_xadd(self, stream: str, fields: dict) -> str:
        return await self.c.streams.xadd(stream, fields)

    async def streams_xlen(self, stream: str) -> int:
        return await self.c.streams.xlen(stream)

    async def streams_xrange(self, stream: str, start: int, end: int, count: int) -> list:
        return await self.c.streams.xrange(stream, start, end, count)

    async def streams_xread(self, stream: str, after: int, count: int) -> list:
        return await self.c.streams.xread(stream, after, count)

    async def streams_xgroupCreate(self, stream: str, group: str, start: int) -> Any:
        return await self.c.streams.xgroup_create(stream, group, start)

    async def streams_xreadgroup(
        self, stream: str, group: str, consumer: str, count: int
    ) -> list:
        return await self.c.streams.xreadgroup(stream, group, consumer, count)

    async def streams_xack(self, stream: str, group: str, entry_id: str) -> int:
        return await self.c.streams.xack(stream, group, entry_id)

    # ── blobs ────────────────────────────────────────────────────────────
    async def blob_put(self, bucket: str, key: str, payload_b64: str) -> Any:
        return await self.c.blob.put(bucket, key, base64.b64decode(payload_b64))

    async def blob_get(self, bucket: str, key: str) -> Any:
        data = await self.c.blob.get(bucket, key)
        return None if data is None else base64.b64encode(data).decode()

    async def blob_getMeta(self, bucket: str, key: str) -> Any:
        return await self.c.blob.get_meta(bucket, key)

    async def blob_exists(self, bucket: str, key: str) -> bool:
        return await self.c.blob.exists(bucket, key)

    async def blob_delete(self, bucket: str, key: str) -> bool:
        return await self.c.blob.delete(bucket, key)

    # ── cdc ──────────────────────────────────────────────────────────────
    async def cdc_read(self, after_seq: int, limit: int) -> list:
        return await self.c.cdc.read(after_seq, limit)

    async def cdc_count(self) -> int:
        return await self.c.cdc.count()

    # ── datalog ──────────────────────────────────────────────────────────
    async def datalog_assertFact(self, fact: str) -> str:
        return await self.c.datalog.assert_fact(fact)

    async def datalog_query(self, query: str) -> list:
        return await self.c.datalog.query(query)

    async def datalog_clear(self, predicate: str) -> str:
        return await self.c.datalog.clear(predicate)

    # ── full-text search ─────────────────────────────────────────────────
    async def fts_indexDoc(self, index: str, doc_id: str, fields: dict) -> Any:
        return await self.c.fts.index_doc(index, doc_id, fields)

    async def fts_search(self, index: str, query: str, limit: int) -> list:
        return await self.c.fts.search(index, query, limit=limit)

    # ── vector ───────────────────────────────────────────────────────────
    async def vector_createCollection(self, coll: str, dim: int) -> Any:
        return await self.c.vector.create_collection(coll, dim)

    async def vector_insert(self, coll: str, vec_id: str, values: list) -> Any:
        return await self.c.vector.insert(coll, vec_id, values)

    async def vector_count(self, coll: str) -> int:
        return await self.c.vector.count(coll)

    async def vector_search(self, coll: str, values: list, k: int) -> list:
        return await self.c.vector.search(coll, values, k=k)

    # ── raw sql ──────────────────────────────────────────────────────────
    async def sql_queryScalar(self, query: str, params: list) -> Any:
        return await self.c.sql.fetchval(query, *params)

    async def sql_execute(self, query: str, params: list) -> Any:
        out = await self.c.sql.execute(query, *params)
        # Command tags come back as "UPDATE 1"; the spec compares row counts.
        if isinstance(out, str):
            tail = out.rsplit(" ", 1)[-1]
            return int(tail) if tail.isdigit() else out
        return out

    async def sql_begin(self) -> Any:
        return await self.c.sql.execute("BEGIN")

    async def sql_rollback(self) -> Any:
        return await self.c.sql.execute("ROLLBACK")


async def run_case(case: dict, client: Any, url: str) -> dict:
    fixtures: dict[str, str] = {}
    bound: dict[str, Any] = {}
    ops = Ops(client, url)

    for i, step in enumerate(case["steps"]):
        args = resolve(step.get("args", []), fixtures, bound)
        result = await ops.call(step["op"], args)
        if "bind" in step:
            bound[step["bind"]] = result
        if "expect" in step:
            try:
                check(result, step["expect"])
            except AssertionError as exc:
                raise AssertionError(f"step {i} ({step['op']}): {exc}") from exc
    return {"ok": True}


async def main() -> int:
    if not DATABASE_URL:
        print(
            "::error::NEUTRON_TEST_DATABASE_URL is not set. This suite is only "
            "meaningful against a live engine; refusing to report a green run "
            "for zero executed cases.",
            file=sys.stderr,
        )
        return 1

    spec = json.loads(SPEC.read_text())
    declared_unsupported: dict[str, str] = {}
    if UNSUPPORTED.exists():
        declared_unsupported = json.loads(UNSUPPORTED.read_text()).get("cases", {})

    from neutron.nucleus.client import NucleusClient
    client = await NucleusClient.connect(DATABASE_URL, min_size=1, max_size=4)

    results = []
    try:
        for case in spec["cases"]:
            entry: dict[str, Any] = {"id": case["id"], "model": case["model"]}
            xf = case.get("xfail")
            # An xfail may be scoped to named SDKs: the statement-Describe
            # defect is only observable through a client that describes
            # before binding, and without scoping every other SDK reports
            # xpass forever and the signal is lost.
            expected_fail = bool(xf) and ("python" in xf.get("sdks", ["python"]))
            try:
                await run_case(case, client, DATABASE_URL)
                entry["status"] = "xpass" if expected_fail else "pass"
                if expected_fail:
                    entry["detail"] = (
                        "case is marked xfail but passed — the underlying bug is "
                        "fixed and the xfail note is now false"
                    )
            except Unsupported as exc:
                reason = declared_unsupported.get(case["id"])
                entry["status"] = "unsupported" if reason else "fail"
                entry["detail"] = reason or (
                    f"op {exc} has no mapping and the case is not declared "
                    f"unsupported in unsupported.json"
                )
            except Exception as exc:  # noqa: BLE001 — any failure is a result
                entry["status"] = "xfail" if expected_fail else "fail"
                entry["detail"] = f"{type(exc).__name__}: {exc}"
            results.append(entry)
    finally:
        await client.pool.close()

    doc = {"sdk": "python", "specVersion": spec["specVersion"], "cases": results}
    print(json.dumps(doc, indent=2))

    bad = [r for r in results if r["status"] in ("fail", "xpass")]
    for r in bad:
        print(f"::error::{r['id']}: {r['status']} — {r.get('detail', '')}", file=sys.stderr)
    counts: dict[str, int] = {}
    for r in results:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    print(f"python: {counts}", file=sys.stderr)
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
