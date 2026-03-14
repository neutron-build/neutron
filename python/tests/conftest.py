"""Shared test fixtures."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from neutron.nucleus.client import Features


@pytest.fixture
def nucleus_features():
    return Features(
        is_nucleus=True,
        version="1.0.0",
        has_kv=True,
        has_vector=True,
        has_ts=True,
        has_document=True,
        has_graph=True,
        has_fts=True,
        has_geo=True,
        has_blob=True,
        has_streams=True,
        has_columnar=True,
        has_datalog=True,
        has_cdc=True,
        has_pubsub=True,
    )


@pytest.fixture
def plain_features():
    return Features()


@pytest.fixture
def mock_conn():
    """A mock asyncpg connection that records calls."""
    conn = AsyncMock()
    conn.fetchval = AsyncMock(return_value=None)
    conn.fetch = AsyncMock(return_value=[])
    conn.fetchrow = AsyncMock(return_value=None)
    conn.execute = AsyncMock(return_value="OK")
    return conn
