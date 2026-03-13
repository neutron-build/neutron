"""Tests for all Nucleus data models (mocked — no real DB required)."""

from __future__ import annotations

import json

import pytest

from neutron.error import AppError
from neutron.nucleus._exec import Executor
from neutron.nucleus.blob import BlobModel
from neutron.nucleus.client import Features
from neutron.nucleus.document import DocumentModel
from neutron.nucleus.fts import FTSModel
from neutron.nucleus.geo import GeoFeature, GeoModel
from neutron.nucleus.graph import GraphModel
from neutron.nucleus.kv import KVModel
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
