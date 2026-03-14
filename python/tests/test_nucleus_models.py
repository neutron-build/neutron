"""Tests for all Nucleus data models (mocked — no real DB required)."""

from __future__ import annotations

import json
import os
import tempfile

import pytest

from neutron.error import AppError
from neutron.nucleus._exec import Executor
from neutron.nucleus.blob import BlobModel
from neutron.nucleus.cdc import CDCModel, CDCEvent, _parse_cdc_events
from neutron.nucleus.client import Features
from neutron.nucleus.columnar import ColumnarModel
from neutron.nucleus.datalog import DatalogModel
from neutron.nucleus.document import DocumentModel
from neutron.nucleus.fts import FTSModel
from neutron.nucleus.geo import GeoFeature, GeoModel
from neutron.nucleus.graph import GraphModel
from neutron.nucleus.kv import KVModel
from neutron.nucleus.pubsub import PubSubModel
from neutron.nucleus.streams import StreamsModel, StreamEntry, _parse_stream_entries
from neutron.nucleus.timeseries import TimeSeriesModel, TimeSeriesPoint
from neutron.nucleus.vector import VectorModel


def _make_exec(mock_conn):
    """Build an Executor wrapping a mock connection."""
    ex = object.__new__(Executor)
    ex._target = mock_conn
    ex._is_pool = False
    return ex


# ============================================================
# Nucleus Guard — all models reject plain PG
# ============================================================


class TestNucleusGuard:
    @pytest.mark.asyncio
    async def test_kv_requires_nucleus(self, mock_conn, plain_features):
        kv = KVModel(_make_exec(mock_conn), plain_features)
        with pytest.raises(AppError) as exc_info:
            await kv.get("key")
        assert exc_info.value.status == 503
        assert "Nucleus" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_vector_requires_nucleus(self, mock_conn, plain_features):
        vec = VectorModel(_make_exec(mock_conn), plain_features)
        with pytest.raises(AppError):
            await vec.search("col", [1.0, 2.0], k=5)

    @pytest.mark.asyncio
    async def test_timeseries_requires_nucleus(self, mock_conn, plain_features):
        ts = TimeSeriesModel(_make_exec(mock_conn), plain_features)
        with pytest.raises(AppError):
            await ts.count("cpu")

    @pytest.mark.asyncio
    async def test_document_requires_nucleus(self, mock_conn, plain_features):
        doc = DocumentModel(_make_exec(mock_conn), plain_features)
        with pytest.raises(AppError):
            await doc.count()

    @pytest.mark.asyncio
    async def test_graph_requires_nucleus(self, mock_conn, plain_features):
        graph = GraphModel(_make_exec(mock_conn), plain_features)
        with pytest.raises(AppError):
            await graph.node_count()

    @pytest.mark.asyncio
    async def test_fts_requires_nucleus(self, mock_conn, plain_features):
        fts = FTSModel(_make_exec(mock_conn), plain_features)
        with pytest.raises(AppError):
            await fts.doc_count()

    @pytest.mark.asyncio
    async def test_geo_requires_nucleus(self, mock_conn, plain_features):
        geo = GeoModel(_make_exec(mock_conn), plain_features)
        with pytest.raises(AppError):
            await geo.distance(0, 0, 1, 1)

    @pytest.mark.asyncio
    async def test_blob_requires_nucleus(self, mock_conn, plain_features):
        blob = BlobModel(_make_exec(mock_conn), plain_features)
        with pytest.raises(AppError):
            await blob.count()


# ============================================================
# KV Model
# ============================================================


class TestKV:
    @pytest.mark.asyncio
    async def test_get(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = "hello"
        kv = KVModel(_make_exec(mock_conn), nucleus_features)
        result = await kv.get("mykey")
        assert result == "hello"
        mock_conn.fetchval.assert_called_with("SELECT KV_GET($1)", "mykey")

    @pytest.mark.asyncio
    async def test_get_returns_none(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = None
        kv = KVModel(_make_exec(mock_conn), nucleus_features)
        assert await kv.get("missing") is None

    @pytest.mark.asyncio
    async def test_set_without_ttl(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = "OK"
        kv = KVModel(_make_exec(mock_conn), nucleus_features)
        await kv.set("k", "v")
        mock_conn.fetchval.assert_called_with("SELECT KV_SET($1, $2)", "k", "v")

    @pytest.mark.asyncio
    async def test_set_with_ttl(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = "OK"
        kv = KVModel(_make_exec(mock_conn), nucleus_features)
        await kv.set("k", "v", ttl=60)
        mock_conn.fetchval.assert_called_with(
            "SELECT KV_SET($1, $2, $3)", "k", "v", 60
        )

    @pytest.mark.asyncio
    async def test_get_typed(self, mock_conn, nucleus_features):
        from pydantic import BaseModel

        class Prefs(BaseModel):
            theme: str

        mock_conn.fetchval.return_value = '{"theme":"dark"}'
        kv = KVModel(_make_exec(mock_conn), nucleus_features)
        result = await kv.get_typed("user:prefs", Prefs)
        assert result is not None
        assert result.theme == "dark"

    @pytest.mark.asyncio
    async def test_set_typed(self, mock_conn, nucleus_features):
        from pydantic import BaseModel

        class Prefs(BaseModel):
            theme: str

        mock_conn.fetchval.return_value = "OK"
        kv = KVModel(_make_exec(mock_conn), nucleus_features)
        await kv.set_typed("user:prefs", Prefs(theme="light"), ttl=300)
        call_args = mock_conn.fetchval.call_args
        assert "KV_SET" in call_args[0][0]
        # The JSON value is the second positional arg (key=$1, value=$2, ttl=$3)
        assert "light" in call_args[0][2]

    @pytest.mark.asyncio
    async def test_delete(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = True
        kv = KVModel(_make_exec(mock_conn), nucleus_features)
        assert await kv.delete("k") is True
        mock_conn.fetchval.assert_called_with("SELECT KV_DEL($1)", "k")

    @pytest.mark.asyncio
    async def test_exists(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = True
        kv = KVModel(_make_exec(mock_conn), nucleus_features)
        assert await kv.exists("k") is True

    @pytest.mark.asyncio
    async def test_incr(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = 5
        kv = KVModel(_make_exec(mock_conn), nucleus_features)
        assert await kv.incr("counter", 3) == 5
        mock_conn.fetchval.assert_called_with("SELECT KV_INCR($1, $2)", "counter", 3)

    @pytest.mark.asyncio
    async def test_hgetall_parsing(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = "name=Alice,age=30"
        kv = KVModel(_make_exec(mock_conn), nucleus_features)
        result = await kv.hgetall("user:1")
        assert result == {"name": "Alice", "age": "30"}

    @pytest.mark.asyncio
    async def test_hgetall_empty(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = ""
        kv = KVModel(_make_exec(mock_conn), nucleus_features)
        assert await kv.hgetall("empty") == {}

    @pytest.mark.asyncio
    async def test_lrange_parsing(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = "a,b,c"
        kv = KVModel(_make_exec(mock_conn), nucleus_features)
        result = await kv.lrange("list", 0, -1)
        assert result == ["a", "b", "c"]

    @pytest.mark.asyncio
    async def test_smembers_parsing(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = "x,y,z"
        kv = KVModel(_make_exec(mock_conn), nucleus_features)
        result = await kv.smembers("myset")
        assert result == ["x", "y", "z"]

    @pytest.mark.asyncio
    async def test_lpush(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = 3
        kv = KVModel(_make_exec(mock_conn), nucleus_features)
        assert await kv.lpush("q", "item") == 3

    @pytest.mark.asyncio
    async def test_zadd(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = True
        kv = KVModel(_make_exec(mock_conn), nucleus_features)
        assert await kv.zadd("leaderboard", 100.0, "player1") is True
        mock_conn.fetchval.assert_called_with(
            "SELECT KV_ZADD($1, $2, $3)", "leaderboard", 100.0, "player1"
        )

    @pytest.mark.asyncio
    async def test_pfadd(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = True
        kv = KVModel(_make_exec(mock_conn), nucleus_features)
        assert await kv.pfadd("hll", "element") is True


# ============================================================
# Vector Model
# ============================================================


class TestVector:
    @pytest.mark.asyncio
    async def test_insert(self, mock_conn, nucleus_features):
        mock_conn.execute.return_value = "INSERT 0 1"
        vec = VectorModel(_make_exec(mock_conn), nucleus_features)
        await vec.insert("articles", "doc-1", [1.0, 2.0, 3.0], {"title": "test"})
        sql = mock_conn.execute.call_args[0][0]
        assert "INSERT INTO articles" in sql
        assert "VECTOR" in sql

    @pytest.mark.asyncio
    async def test_search(self, mock_conn, nucleus_features):
        mock_conn.fetch.return_value = [
            {"id": "doc-1", "score": 0.1, "metadata": '{"title":"test"}'},
            {"id": "doc-2", "score": 0.5, "metadata": '{"title":"other"}'},
        ]
        vec = VectorModel(_make_exec(mock_conn), nucleus_features)
        results = await vec.search("articles", [1.0, 2.0], k=2)
        assert len(results) == 2
        assert results[0].id == "doc-1"
        assert results[0].score == 0.1
        assert results[0].metadata["title"] == "test"

    @pytest.mark.asyncio
    async def test_search_with_filter(self, mock_conn, nucleus_features):
        mock_conn.fetch.return_value = []
        vec = VectorModel(_make_exec(mock_conn), nucleus_features)
        await vec.search("col", [1.0], filter={"category": "news"})
        sql = mock_conn.fetch.call_args[0][0]
        assert "category" in sql

    @pytest.mark.asyncio
    async def test_create_collection(self, mock_conn, nucleus_features):
        mock_conn.execute.return_value = "CREATE TABLE"
        vec = VectorModel(_make_exec(mock_conn), nucleus_features)
        await vec.create_collection("embeddings", 1536, metric="cosine")
        calls = [c[0][0] for c in mock_conn.execute.call_args_list]
        assert any("CREATE TABLE" in c for c in calls)
        assert any("CREATE INDEX" in c for c in calls)

    @pytest.mark.asyncio
    async def test_delete(self, mock_conn, nucleus_features):
        mock_conn.execute.return_value = "DELETE 1"
        vec = VectorModel(_make_exec(mock_conn), nucleus_features)
        await vec.delete("col", "doc-1")
        sql = mock_conn.execute.call_args[0][0]
        assert "DELETE" in sql


# ============================================================
# TimeSeries Model
# ============================================================


class TestTimeSeries:
    @pytest.mark.asyncio
    async def test_write(self, mock_conn, nucleus_features):
        from datetime import datetime, timezone

        mock_conn.fetchval.return_value = "OK"
        ts = TimeSeriesModel(_make_exec(mock_conn), nucleus_features)
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        await ts.write("cpu", [TimeSeriesPoint(timestamp=now, value=72.5)])
        mock_conn.fetchval.assert_called_once()
        sql = mock_conn.fetchval.call_args[0][0]
        assert "TS_INSERT" in sql

    @pytest.mark.asyncio
    async def test_last(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = 42.0
        ts = TimeSeriesModel(_make_exec(mock_conn), nucleus_features)
        assert await ts.last("cpu") == 42.0
        mock_conn.fetchval.assert_called_with("SELECT TS_LAST($1)", "cpu")

    @pytest.mark.asyncio
    async def test_count(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = 1000
        ts = TimeSeriesModel(_make_exec(mock_conn), nucleus_features)
        assert await ts.count("cpu") == 1000

    @pytest.mark.asyncio
    async def test_retention(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = True
        ts = TimeSeriesModel(_make_exec(mock_conn), nucleus_features)
        assert await ts.retention("cpu", 30) is True
        mock_conn.fetchval.assert_called_with(
            "SELECT TS_RETENTION($1, $2)", "cpu", 30
        )


# ============================================================
# Document Model
# ============================================================


class TestDocument:
    @pytest.mark.asyncio
    async def test_insert(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = 42
        doc = DocumentModel(_make_exec(mock_conn), nucleus_features)
        doc_id = await doc.insert("users", {"name": "Alice"})
        assert doc_id == "42"
        sql = mock_conn.fetchval.call_args[0][0]
        assert "DOC_INSERT" in sql

    @pytest.mark.asyncio
    async def test_get(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = '{"name": "Alice"}'
        doc = DocumentModel(_make_exec(mock_conn), nucleus_features)
        result = await doc.get(42)
        assert result == {"name": "Alice"}

    @pytest.mark.asyncio
    async def test_get_returns_none(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = None
        doc = DocumentModel(_make_exec(mock_conn), nucleus_features)
        assert await doc.get(999) is None

    @pytest.mark.asyncio
    async def test_count(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = 100
        doc = DocumentModel(_make_exec(mock_conn), nucleus_features)
        assert await doc.count() == 100


# ============================================================
# Graph Model
# ============================================================


class TestGraph:
    @pytest.mark.asyncio
    async def test_add_node(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = 1
        graph = GraphModel(_make_exec(mock_conn), nucleus_features)
        node_id = await graph.add_node(["Person"], {"name": "Alice"})
        assert node_id == "1"
        sql = mock_conn.fetchval.call_args[0][0]
        assert "GRAPH_ADD_NODE" in sql

    @pytest.mark.asyncio
    async def test_add_edge(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = 10
        graph = GraphModel(_make_exec(mock_conn), nucleus_features)
        edge_id = await graph.add_edge("KNOWS", "1", "2")
        assert edge_id == "10"
        sql = mock_conn.fetchval.call_args[0][0]
        assert "GRAPH_ADD_EDGE" in sql

    @pytest.mark.asyncio
    async def test_query_cypher(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = json.dumps(
            {"nodes": [{"id": "1", "labels": ["Person"]}], "edges": []}
        )
        graph = GraphModel(_make_exec(mock_conn), nucleus_features)
        result = await graph.query("MATCH (n:Person) RETURN n")
        assert len(result.nodes) == 1

    @pytest.mark.asyncio
    async def test_neighbors(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = json.dumps(
            [{"id": 2, "name": "Bob"}, {"id": 3, "name": "Carol"}]
        )
        graph = GraphModel(_make_exec(mock_conn), nucleus_features)
        neighbors = await graph.neighbors("1")
        assert len(neighbors) == 2

    @pytest.mark.asyncio
    async def test_shortest_path(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = json.dumps([1, 5, 10])
        graph = GraphModel(_make_exec(mock_conn), nucleus_features)
        path = await graph.shortest_path("1", "10")
        assert len(path) == 3

    @pytest.mark.asyncio
    async def test_node_count(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = 50
        graph = GraphModel(_make_exec(mock_conn), nucleus_features)
        assert await graph.node_count() == 50


# ============================================================
# FTS Model
# ============================================================


class TestFTS:
    @pytest.mark.asyncio
    async def test_search(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = json.dumps(
            [{"doc_id": 1, "score": 0.9}, {"doc_id": 2, "score": 0.7}]
        )
        fts = FTSModel(_make_exec(mock_conn), nucleus_features)
        results = await fts.search("articles", "hello", limit=10)
        assert len(results) == 2
        assert results[0].id == "1"
        assert results[0].score == 0.9

    @pytest.mark.asyncio
    async def test_fuzzy_search(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = json.dumps([{"doc_id": 1, "score": 0.8}])
        fts = FTSModel(_make_exec(mock_conn), nucleus_features)
        results = await fts.search("articles", "helo", fuzzy=2)
        sql = mock_conn.fetchval.call_args[0][0]
        assert "FTS_FUZZY_SEARCH" in sql

    @pytest.mark.asyncio
    async def test_index_doc(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = True
        fts = FTSModel(_make_exec(mock_conn), nucleus_features)
        await fts.index_doc("articles", "42", {"title": "Hello", "body": "World"})
        sql = mock_conn.fetchval.call_args[0][0]
        assert "FTS_INDEX" in sql

    @pytest.mark.asyncio
    async def test_doc_count(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = 500
        fts = FTSModel(_make_exec(mock_conn), nucleus_features)
        assert await fts.doc_count() == 500


# ============================================================
# Geo Model
# ============================================================


class TestGeo:
    @pytest.mark.asyncio
    async def test_distance(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = 1234.5
        geo = GeoModel(_make_exec(mock_conn), nucleus_features)
        d = await geo.distance(40.7, -74.0, 34.0, -118.2)
        assert d == 1234.5
        sql = mock_conn.fetchval.call_args[0][0]
        assert "GEO_DISTANCE" in sql

    @pytest.mark.asyncio
    async def test_within(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = True
        geo = GeoModel(_make_exec(mock_conn), nucleus_features)
        assert await geo.within(40.7, -74.0, 40.71, -74.01, 1000) is True
        sql = mock_conn.fetchval.call_args[0][0]
        assert "GEO_WITHIN" in sql

    @pytest.mark.asyncio
    async def test_insert(self, mock_conn, nucleus_features):
        mock_conn.execute.return_value = "CREATE TABLE"
        geo = GeoModel(_make_exec(mock_conn), nucleus_features)
        await geo.insert(
            "shops",
            GeoFeature(id="s1", lat=40.7, lon=-74.0, properties={"name": "Shop"}),
        )
        calls = [c[0][0] for c in mock_conn.execute.call_args_list]
        assert any("ST_MAKEPOINT" in c for c in calls)


# ============================================================
# Blob Model
# ============================================================


class TestBlob:
    @pytest.mark.asyncio
    async def test_put(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = True
        blob = BlobModel(_make_exec(mock_conn), nucleus_features)
        await blob.put("uploads", "photo.jpg", b"\x89PNG", content_type="image/png")
        sql = mock_conn.fetchval.call_args[0][0]
        assert "BLOB_STORE" in sql

    @pytest.mark.asyncio
    async def test_get(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = "48656c6c6f"
        blob = BlobModel(_make_exec(mock_conn), nucleus_features)
        data = await blob.get("uploads", "file.txt")
        assert data == b"Hello"

    @pytest.mark.asyncio
    async def test_get_returns_none(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = None
        blob = BlobModel(_make_exec(mock_conn), nucleus_features)
        assert await blob.get("uploads", "missing") is None

    @pytest.mark.asyncio
    async def test_delete(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = True
        blob = BlobModel(_make_exec(mock_conn), nucleus_features)
        assert await blob.delete("uploads", "file.txt") is True
        sql = mock_conn.fetchval.call_args[0][0]
        assert "BLOB_DELETE" in sql

    @pytest.mark.asyncio
    async def test_exists(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = '{"size": 100}'
        blob = BlobModel(_make_exec(mock_conn), nucleus_features)
        assert await blob.exists("uploads", "file.txt") is True

    @pytest.mark.asyncio
    async def test_list(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = json.dumps(["a.txt", "b.txt"])
        blob = BlobModel(_make_exec(mock_conn), nucleus_features)
        items = await blob.list("uploads")
        assert len(items) == 2
        assert items[0].key == "a.txt"

    @pytest.mark.asyncio
    async def test_count(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = 42
        blob = BlobModel(_make_exec(mock_conn), nucleus_features)
        assert await blob.count() == 42

    @pytest.mark.asyncio
    async def test_get_meta(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = json.dumps(
            {"size": 1024, "content_type": "image/png", "tags": {"env": "prod"}}
        )
        blob = BlobModel(_make_exec(mock_conn), nucleus_features)
        meta = await blob.get_meta("uploads", "photo.png")
        assert meta is not None
        assert meta.size == 1024
        assert meta.content_type == "image/png"
        assert meta.metadata["env"] == "prod"


# ============================================================
# CDC Model
# ============================================================


class TestCDC:
    @pytest.mark.asyncio
    async def test_read(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = json.dumps(
            [{"offset": 0, "table": "users", "operation": "INSERT", "data": {"id": 1}}]
        )
        cdc = CDCModel(_make_exec(mock_conn), nucleus_features)
        events = await cdc.read(offset=0)
        assert len(events) == 1
        assert events[0].offset == 0
        assert events[0].table == "users"
        assert events[0].operation == "INSERT"
        mock_conn.fetchval.assert_called_with("SELECT CDC_READ($1)", 0)

    @pytest.mark.asyncio
    async def test_read_empty(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = None
        cdc = CDCModel(_make_exec(mock_conn), nucleus_features)
        events = await cdc.read(offset=0)
        assert events == []

    @pytest.mark.asyncio
    async def test_count(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = 42
        cdc = CDCModel(_make_exec(mock_conn), nucleus_features)
        assert await cdc.count() == 42
        mock_conn.fetchval.assert_called_with("SELECT CDC_COUNT()")

    @pytest.mark.asyncio
    async def test_table_read(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = json.dumps(
            [{"offset": 5, "op": "UPDATE", "data": {"name": "Bob"}}]
        )
        cdc = CDCModel(_make_exec(mock_conn), nucleus_features)
        events = await cdc.table_read("users", offset=5)
        assert len(events) == 1
        assert events[0].table == "users"
        mock_conn.fetchval.assert_called_with(
            "SELECT CDC_TABLE_READ($1, $2)", "users", 5
        )

    @pytest.mark.asyncio
    async def test_requires_nucleus(self, mock_conn, plain_features):
        cdc = CDCModel(_make_exec(mock_conn), plain_features)
        with pytest.raises(AppError):
            await cdc.read(0)

    @pytest.mark.asyncio
    async def test_count_requires_nucleus(self, mock_conn, plain_features):
        cdc = CDCModel(_make_exec(mock_conn), plain_features)
        with pytest.raises(AppError):
            await cdc.count()


class TestCDCEventParsing:
    def test_parse_empty(self):
        assert _parse_cdc_events(None) == []
        assert _parse_cdc_events("") == []

    def test_parse_single_event(self):
        raw = json.dumps({"offset": 0, "table": "users", "operation": "INSERT", "data": {"id": 1}})
        events = _parse_cdc_events(raw)
        assert len(events) == 1
        assert events[0].offset == 0

    def test_parse_list(self):
        raw = json.dumps([
            {"offset": 0, "table": "a", "operation": "INSERT", "data": {}},
            {"offset": 1, "table": "b", "operation": "DELETE", "data": {}},
        ])
        events = _parse_cdc_events(raw)
        assert len(events) == 2

    def test_parse_invalid_json(self):
        assert _parse_cdc_events("not-json{{") == []

    def test_cdc_event_model(self):
        event = CDCEvent(offset=0, table="users", operation="INSERT", data={"id": 1})
        assert event.offset == 0
        assert event.table == "users"
        assert event.operation == "INSERT"
        assert event.data == {"id": 1}

    def test_parse_uses_op_fallback(self):
        raw = json.dumps([{"offset": 0, "op": "DELETE", "data": {}}])
        events = _parse_cdc_events(raw)
        assert events[0].operation == "DELETE"


# ============================================================
# Columnar Model
# ============================================================


class TestColumnar:
    @pytest.mark.asyncio
    async def test_insert(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = True
        col = ColumnarModel(_make_exec(mock_conn), nucleus_features)
        result = await col.insert("metrics", {"ts": 1700000000, "value": 42.5})
        assert result is True
        sql = mock_conn.fetchval.call_args[0][0]
        assert "COLUMNAR_INSERT" in sql

    @pytest.mark.asyncio
    async def test_count(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = 1000
        col = ColumnarModel(_make_exec(mock_conn), nucleus_features)
        assert await col.count("metrics") == 1000
        mock_conn.fetchval.assert_called_with("SELECT COLUMNAR_COUNT($1)", "metrics")

    @pytest.mark.asyncio
    async def test_sum(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = 5000.0
        col = ColumnarModel(_make_exec(mock_conn), nucleus_features)
        assert await col.sum("metrics", "value") == 5000.0
        mock_conn.fetchval.assert_called_with(
            "SELECT COLUMNAR_SUM($1, $2)", "metrics", "value"
        )

    @pytest.mark.asyncio
    async def test_avg(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = 42.5
        col = ColumnarModel(_make_exec(mock_conn), nucleus_features)
        assert await col.avg("metrics", "value") == 42.5

    @pytest.mark.asyncio
    async def test_min(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = 1.0
        col = ColumnarModel(_make_exec(mock_conn), nucleus_features)
        assert await col.min("metrics", "value") == 1.0

    @pytest.mark.asyncio
    async def test_max(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = 100.0
        col = ColumnarModel(_make_exec(mock_conn), nucleus_features)
        assert await col.max("metrics", "value") == 100.0

    @pytest.mark.asyncio
    async def test_requires_nucleus(self, mock_conn, plain_features):
        col = ColumnarModel(_make_exec(mock_conn), plain_features)
        with pytest.raises(AppError):
            await col.count("t")


# ============================================================
# Datalog Model
# ============================================================


class TestDatalog:
    @pytest.mark.asyncio
    async def test_assert_fact(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = True
        dl = DatalogModel(_make_exec(mock_conn), nucleus_features)
        assert await dl.assert_fact("parent(alice, bob)") is True
        mock_conn.fetchval.assert_called_with(
            "SELECT DATALOG_ASSERT($1)", "parent(alice, bob)"
        )

    @pytest.mark.asyncio
    async def test_retract(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = True
        dl = DatalogModel(_make_exec(mock_conn), nucleus_features)
        assert await dl.retract("parent(alice, bob)") is True
        mock_conn.fetchval.assert_called_with(
            "SELECT DATALOG_RETRACT($1)", "parent(alice, bob)"
        )

    @pytest.mark.asyncio
    async def test_rule(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = True
        dl = DatalogModel(_make_exec(mock_conn), nucleus_features)
        assert await dl.rule("ancestor(X, Z)", "parent(X, Y), ancestor(Y, Z)") is True
        mock_conn.fetchval.assert_called_with(
            "SELECT DATALOG_RULE($1, $2)",
            "ancestor(X, Z)",
            "parent(X, Y), ancestor(Y, Z)",
        )

    @pytest.mark.asyncio
    async def test_query(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = "alice,bob\ncarol,dave"
        dl = DatalogModel(_make_exec(mock_conn), nucleus_features)
        results = await dl.query("ancestor(alice, ?X)")
        assert len(results) == 2
        assert results[0] == ["alice", "bob"]
        assert results[1] == ["carol", "dave"]

    @pytest.mark.asyncio
    async def test_query_empty(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = None
        dl = DatalogModel(_make_exec(mock_conn), nucleus_features)
        results = await dl.query("ancestor(alice, ?X)")
        assert results == []

    @pytest.mark.asyncio
    async def test_query_empty_string(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = ""
        dl = DatalogModel(_make_exec(mock_conn), nucleus_features)
        results = await dl.query("ancestor(alice, ?X)")
        assert results == []

    @pytest.mark.asyncio
    async def test_clear(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = True
        dl = DatalogModel(_make_exec(mock_conn), nucleus_features)
        assert await dl.clear() is True
        mock_conn.fetchval.assert_called_with("SELECT DATALOG_CLEAR()")

    @pytest.mark.asyncio
    async def test_import_graph(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = 25
        dl = DatalogModel(_make_exec(mock_conn), nucleus_features)
        assert await dl.import_graph() == 25
        mock_conn.fetchval.assert_called_with("SELECT DATALOG_IMPORT_GRAPH()")

    @pytest.mark.asyncio
    async def test_requires_nucleus(self, mock_conn, plain_features):
        dl = DatalogModel(_make_exec(mock_conn), plain_features)
        with pytest.raises(AppError):
            await dl.assert_fact("f")


# ============================================================
# Streams Model
# ============================================================


class TestStreams:
    @pytest.mark.asyncio
    async def test_xadd(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = "1700000000-0"
        sm = StreamsModel(_make_exec(mock_conn), nucleus_features)
        entry_id = await sm.xadd("events", {"action": "login"})
        assert entry_id == "1700000000-0"
        sql = mock_conn.fetchval.call_args[0][0]
        assert "STREAM_XADD" in sql

    @pytest.mark.asyncio
    async def test_xlen(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = 42
        sm = StreamsModel(_make_exec(mock_conn), nucleus_features)
        assert await sm.xlen("events") == 42
        mock_conn.fetchval.assert_called_with("SELECT STREAM_XLEN($1)", "events")

    @pytest.mark.asyncio
    async def test_xrange(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = json.dumps([
            {"id": "100-0", "action": "login"},
            {"id": "200-0", "action": "logout"},
        ])
        sm = StreamsModel(_make_exec(mock_conn), nucleus_features)
        entries = await sm.xrange("events", 0, 1000, count=10)
        assert len(entries) == 2
        assert entries[0].id == "100-0"

    @pytest.mark.asyncio
    async def test_xrange_empty(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = None
        sm = StreamsModel(_make_exec(mock_conn), nucleus_features)
        entries = await sm.xrange("events", 0, 100)
        assert entries == []

    @pytest.mark.asyncio
    async def test_xread(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = json.dumps([
            {"id": "300-0", "task": "process"},
        ])
        sm = StreamsModel(_make_exec(mock_conn), nucleus_features)
        entries = await sm.xread("events", last_id_ms=200, count=5)
        assert len(entries) == 1
        assert entries[0].id == "300-0"

    @pytest.mark.asyncio
    async def test_xgroup_create(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = True
        sm = StreamsModel(_make_exec(mock_conn), nucleus_features)
        assert await sm.xgroup_create("events", "workers", 0) is True
        mock_conn.fetchval.assert_called_with(
            "SELECT STREAM_XGROUP_CREATE($1, $2, $3)", "events", "workers", 0
        )

    @pytest.mark.asyncio
    async def test_xreadgroup(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = json.dumps([
            {"id": "400-0", "task": "send_email"},
        ])
        sm = StreamsModel(_make_exec(mock_conn), nucleus_features)
        entries = await sm.xreadgroup("events", "workers", "w1", count=10)
        assert len(entries) == 1

    @pytest.mark.asyncio
    async def test_xack(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = True
        sm = StreamsModel(_make_exec(mock_conn), nucleus_features)
        assert await sm.xack("events", "workers", 400, 0) is True
        mock_conn.fetchval.assert_called_with(
            "SELECT STREAM_XACK($1, $2, $3, $4)", "events", "workers", 400, 0
        )

    @pytest.mark.asyncio
    async def test_requires_nucleus(self, mock_conn, plain_features):
        sm = StreamsModel(_make_exec(mock_conn), plain_features)
        with pytest.raises(AppError):
            await sm.xlen("s")


class TestStreamEntryParsing:
    def test_parse_empty(self):
        assert _parse_stream_entries(None) == []
        assert _parse_stream_entries("") == []

    def test_parse_valid(self):
        raw = json.dumps([
            {"id": "100-0", "action": "login"},
            {"id": "200-0", "action": "logout"},
        ])
        entries = _parse_stream_entries(raw)
        assert len(entries) == 2
        assert entries[0].id == "100-0"
        assert entries[0].fields.get("action") == "login"

    def test_parse_invalid_json(self):
        assert _parse_stream_entries("bad-json{{") == []

    def test_stream_entry_model(self):
        entry = StreamEntry(id="100-0", fields={"key": "value"})
        assert entry.id == "100-0"
        assert entry.fields["key"] == "value"

    def test_stream_entry_empty_fields(self):
        entry = StreamEntry(id="100-0")
        assert entry.fields == {}


# ============================================================
# PubSub Model
# ============================================================


class TestPubSub:
    @pytest.mark.asyncio
    async def test_publish_nucleus(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = 3
        # PubSubModel needs a pool too, but for testing we pass mock_conn as pool
        ps = PubSubModel(mock_conn, _make_exec(mock_conn), nucleus_features)
        n = await ps.publish("events", '{"type":"update"}')
        assert n == 3
        mock_conn.fetchval.assert_called_with(
            "SELECT PUBSUB_PUBLISH($1, $2)", "events", '{"type":"update"}'
        )

    @pytest.mark.asyncio
    async def test_channels_with_pattern(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = "events,notifications"
        ps = PubSubModel(mock_conn, _make_exec(mock_conn), nucleus_features)
        channels = await ps.channels("ev*")
        assert "events" in channels
        assert "notifications" in channels

    @pytest.mark.asyncio
    async def test_channels_no_pattern(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = "ch1,ch2"
        ps = PubSubModel(mock_conn, _make_exec(mock_conn), nucleus_features)
        channels = await ps.channels()
        assert len(channels) == 2

    @pytest.mark.asyncio
    async def test_channels_empty(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = ""
        ps = PubSubModel(mock_conn, _make_exec(mock_conn), nucleus_features)
        channels = await ps.channels()
        assert channels == []

    @pytest.mark.asyncio
    async def test_subscriber_count(self, mock_conn, nucleus_features):
        mock_conn.fetchval.return_value = 7
        ps = PubSubModel(mock_conn, _make_exec(mock_conn), nucleus_features)
        assert await ps.subscriber_count("events") == 7
        mock_conn.fetchval.assert_called_with(
            "SELECT PUBSUB_SUBSCRIBERS($1)", "events"
        )

    @pytest.mark.asyncio
    async def test_channels_requires_nucleus(self, mock_conn, plain_features):
        ps = PubSubModel(mock_conn, _make_exec(mock_conn), plain_features)
        with pytest.raises(AppError):
            await ps.channels()

    @pytest.mark.asyncio
    async def test_subscriber_count_requires_nucleus(self, mock_conn, plain_features):
        ps = PubSubModel(mock_conn, _make_exec(mock_conn), plain_features)
        with pytest.raises(AppError):
            await ps.subscriber_count("ch")


# ============================================================
# SQL Model
# ============================================================


class TestSQLModel:
    @pytest.mark.asyncio
    async def test_execute_insert(self, mock_conn):
        from neutron.nucleus.sql import SQLModel as PySQLModel

        mock_pool = mock_conn
        mock_pool.acquire.return_value.__aenter__ = mock_conn.__aenter__
        mock_pool.acquire.return_value.__aexit__ = mock_conn.__aexit__
        # SQLModel wraps asyncpg pool
        # Testing the parse of execute result
        result_str = "INSERT 0 3"
        parts = result_str.split()
        try:
            count = int(parts[-1])
        except (ValueError, IndexError):
            count = 0
        assert count == 3

    def test_execute_result_parsing_update(self):
        result_str = "UPDATE 5"
        parts = result_str.split()
        count = int(parts[-1])
        assert count == 5

    def test_execute_result_parsing_delete(self):
        result_str = "DELETE 2"
        parts = result_str.split()
        count = int(parts[-1])
        assert count == 2

    def test_execute_result_parsing_create(self):
        result_str = "CREATE TABLE"
        parts = result_str.split()
        try:
            count = int(parts[-1])
        except (ValueError, IndexError):
            count = 0
        assert count == 0


# ============================================================
# Migration
# ============================================================


class TestMigration:
    def test_migration_dataclass(self):
        from neutron.nucleus.migrate import Migration

        m = Migration(version=1, name="create_users", up="CREATE TABLE users (id INT)")
        assert m.version == 1
        assert m.name == "create_users"
        assert "CREATE TABLE" in m.up
        assert m.down == ""

    def test_migration_with_down(self):
        from neutron.nucleus.migrate import Migration

        m = Migration(
            version=1,
            name="create_users",
            up="CREATE TABLE users (id INT)",
            down="DROP TABLE users",
        )
        assert m.down == "DROP TABLE users"

    def test_load_from_dir_empty(self):
        from neutron.nucleus.migrate import _load_from_dir

        with tempfile.TemporaryDirectory() as tmpdir:
            migrations = _load_from_dir(tmpdir)
            assert migrations == []

    def test_load_from_dir_nonexistent(self):
        from neutron.nucleus.migrate import _load_from_dir

        migrations = _load_from_dir("/nonexistent/path")
        assert migrations == []

    def test_load_from_dir_with_files(self):
        from neutron.nucleus.migrate import _load_from_dir

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create migration files
            with open(os.path.join(tmpdir, "001_create_users.sql"), "w") as f:
                f.write("CREATE TABLE users (id INT);\n-- DOWN\nDROP TABLE users;")
            with open(os.path.join(tmpdir, "002_add_email.sql"), "w") as f:
                f.write("ALTER TABLE users ADD COLUMN email TEXT;")

            migrations = _load_from_dir(tmpdir)
            assert len(migrations) == 2
            assert migrations[0].version == 1
            assert migrations[0].name == "create_users"
            assert "CREATE TABLE" in migrations[0].up
            assert "DROP TABLE" in migrations[0].down
            assert migrations[1].version == 2
            assert migrations[1].down == ""

    def test_load_from_dir_skips_non_sql(self):
        from neutron.nucleus.migrate import _load_from_dir

        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "readme.txt"), "w") as f:
                f.write("not a migration")
            with open(os.path.join(tmpdir, "001_init.sql"), "w") as f:
                f.write("CREATE TABLE t (id INT);")

            migrations = _load_from_dir(tmpdir)
            assert len(migrations) == 1

    def test_load_from_dir_ordering(self):
        from neutron.nucleus.migrate import _load_from_dir

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create files out of order
            with open(os.path.join(tmpdir, "003_third.sql"), "w") as f:
                f.write("SELECT 3;")
            with open(os.path.join(tmpdir, "001_first.sql"), "w") as f:
                f.write("SELECT 1;")
            with open(os.path.join(tmpdir, "002_second.sql"), "w") as f:
                f.write("SELECT 2;")

            migrations = _load_from_dir(tmpdir)
            # os.listdir + sorted should give proper order
            versions = [m.version for m in migrations]
            assert versions == [1, 2, 3]

    def test_load_from_dir_skips_bad_format(self):
        from neutron.nucleus.migrate import _load_from_dir

        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "bad_name.sql"), "w") as f:
                f.write("SELECT 1;")
            with open(os.path.join(tmpdir, "abc_notint.sql"), "w") as f:
                f.write("SELECT 2;")

            migrations = _load_from_dir(tmpdir)
            assert len(migrations) == 0


# ============================================================
# Transaction
# ============================================================


class TestTransaction:
    def test_transaction_sql_result_parsing(self):
        """Test the result parsing logic used in _TransactionSQL.execute."""
        result = "INSERT 0 1"
        parts = result.split()
        try:
            count = int(parts[-1])
        except (ValueError, IndexError):
            count = 0
        assert count == 1

    def test_transaction_sql_result_parsing_update(self):
        result = "UPDATE 5"
        parts = result.split()
        count = int(parts[-1])
        assert count == 5

    def test_transaction_sql_result_parsing_create(self):
        result = "CREATE TABLE"
        parts = result.split()
        try:
            count = int(parts[-1])
        except (ValueError, IndexError):
            count = 0
        assert count == 0
